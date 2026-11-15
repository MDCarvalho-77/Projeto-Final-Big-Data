"""Testes do job Gold de machine learning (pipeline/gold_ml.py, FEAT-004).

Seguem o padrão de tests/pipeline/test_gold_engajamento.py: usam a fixture `spark`
(local[2]) e exercitam as funções puras (`marcar_corte_add_to_cart`,
`montar_features`, `rotular`, `treinar_e_escorar`, `construir_metricas_modelo`,
`montar_features_visitante`, `segmentar_visitantes`) com DataFrames em memória no
schema que `ler_eventos(spark, "silver")` entrega — colunas tipadas de params
(percent_scrolled, video_percent), `event_ts_ts` já timestamp, `session_id`/
`user_pseudo_id`, e os structs `traffic`/`device` usados por `atributos_sessao`.

Cobrem o plano de testabilidade do design §6.5 e os critérios da FEAT-004:
- anti-vazamento: nenhuma feature deriva de evento de checkout nem de evento
  >= t_corte; sessão com purchase mas sem pré-carrinho informativo tem contagens
  de checkout = 0; sessão sem add_to_cart usa todos os pré-checkout;
- rótulo = sessão com purchase;
- split determinístico (seed 42) e propensao_sessao cobre TODAS as sessões com
  probabilidade_compra em [0,1];
- metricas_modelo tem EXATAMENTE 4 linhas (auc, f1, precisao, recall);
- KMeans dá 1 segmento por visitante e gera perfil médio.
"""

from __future__ import annotations

from datetime import datetime

import pytest

import gold_ml as gm
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# --------------------------------------------------------------------------- #
# Schema e helpers
# --------------------------------------------------------------------------- #

_TRAFFIC = StructType([
    StructField("source", StringType()),
    StructField("medium", StringType()),
])
_DEVICE = StructType([
    StructField("category", StringType()),
])

SCHEMA_EVENTOS = StructType([
    StructField("event_id", StringType()),
    StructField("event_name", StringType()),
    StructField("event_ts_ts", TimestampType()),
    StructField("session_id", StringType()),
    StructField("user_pseudo_id", StringType()),
    StructField("user_id_resolvido", StringType()),
    StructField("percent_scrolled", IntegerType()),
    StructField("video_percent", IntegerType()),
    StructField("item_id", StringType()),
    StructField("traffic", _TRAFFIC),
    StructField("device", _DEVICE),
])


def _ts(minuto_segundo: str) -> datetime:
    """'10:00' -> 2026-09-01 10:00:00 (UTC-naive, suficiente para ordenação)."""
    return datetime.fromisoformat(f"2026-09-01T{minuto_segundo}:00")


def _ev(event_id, event_name, session_id, event_ts="10:00",
        user_pseudo_id="anon-1", user_id_resolvido=None,
        percent_scrolled=None, video_percent=None, item_id=None,
        source="google", medium="cpc", category="mobile"):
    return (
        event_id, event_name, _ts(event_ts), session_id,
        user_pseudo_id, user_id_resolvido,
        percent_scrolled, video_percent, item_id,
        (source, medium), (category,),
    )


def _df(spark, linhas):
    return spark.createDataFrame(linhas, SCHEMA_EVENTOS)


# --------------------------------------------------------------------------- #
# §6.1 — Corte anti-vazamento (marcar_corte_add_to_cart)
# --------------------------------------------------------------------------- #

def test_corte_t_corte_e_menor_add_to_cart(spark):
    df = _df(spark, [
        _ev("e1", "page_view", "s-1", "10:00"),
        _ev("e2", "add_to_cart", "s-1", "10:10"),
        _ev("e3", "add_to_cart", "s-1", "10:20"),  # segundo; t_corte = o primeiro
    ])
    marcado = gm.marcar_corte_add_to_cart(df)
    por_id = {l.event_id: l for l in marcado.collect()}
    assert por_id["e1"].t_corte == _ts("10:10")
    # evento anterior ao corte e fora da lista negra -> elegível
    assert por_id["e1"]._elegivel is True
    # o próprio add_to_cart nunca é elegível (lista negra)
    assert por_id["e2"]._elegivel is False
    assert por_id["e3"]._elegivel is False


def test_corte_sessao_sem_add_to_cart_t_corte_nulo(spark):
    df = _df(spark, [
        _ev("e1", "page_view", "s-1", "10:00"),
        _ev("e2", "view_item", "s-1", "10:05"),
    ])
    marcado = gm.marcar_corte_add_to_cart(df)
    for l in marcado.collect():
        assert l.t_corte is None
        assert l._elegivel is True  # sem add_to_cart, todos os pré-checkout elegíveis


def test_corte_eventos_pos_corte_nao_elegiveis(spark):
    df = _df(spark, [
        _ev("e1", "page_view", "s-1", "10:00"),      # pré-corte
        _ev("e2", "add_to_cart", "s-1", "10:10"),    # fronteira
        _ev("e3", "view_item", "s-1", "10:15"),      # pós-corte -> não elegível
    ])
    por_id = {l.event_id: l for l in gm.marcar_corte_add_to_cart(df).collect()}
    assert por_id["e1"]._elegivel is True
    assert por_id["e3"]._elegivel is False


def test_corte_checkout_sempre_fora_mesmo_sem_add_to_cart(spark):
    # sessão sem add_to_cart, mas com eventos de checkout -> lista negra barra.
    df = _df(spark, [
        _ev("e1", "page_view", "s-1", "10:00"),
        _ev("e2", "begin_checkout", "s-1", "10:05"),
        _ev("e3", "purchase", "s-1", "10:10"),
    ])
    por_id = {l.event_id: l for l in gm.marcar_corte_add_to_cart(df).collect()}
    assert por_id["e1"]._elegivel is True
    assert por_id["e2"]._elegivel is False
    assert por_id["e3"]._elegivel is False


# --------------------------------------------------------------------------- #
# §6.2 — Features (montar_features)  — sem vazamento
# --------------------------------------------------------------------------- #

def test_features_uma_linha_por_sessao_e_colunas(spark):
    df = _df(spark, [
        _ev("e1", "page_view", "s-1", "10:00"),
        _ev("e2", "page_view", "s-2", "11:00", user_pseudo_id="anon-2"),
    ])
    features = gm.montar_features(df)
    assert features.count() == 2
    for c in gm.FEATURES_NUMERICAS + gm.FEATURES_CATEGORICAS:
        assert c in features.columns
    assert "session_id" in features.columns


def test_features_contam_so_pre_corte(spark):
    df = _df(spark, [
        _ev("e1", "view_item_list", "s-1", "10:00"),   # pré-corte -> conta
        _ev("e2", "select_item", "s-1", "10:01", item_id="PLANO-PRO"),  # pré -> conta
        _ev("e3", "add_to_cart", "s-1", "10:05"),      # fronteira
        _ev("e4", "view_item", "s-1", "10:06"),        # pós-corte -> NÃO conta
        _ev("e5", "select_item", "s-1", "10:07", item_id="PLANO-PRO"),  # pós -> NÃO
    ])
    linha = gm.montar_features(df).collect()[0]
    assert linha.n_view_item_list == 1
    assert linha.n_select_item == 1      # só o pré-corte (e2), não e5
    assert linha.n_view_item == 0        # e4 é pós-corte
    assert linha.tem_select_item == 1


def test_features_purchase_sem_pre_carrinho_zera_checkout(spark):
    # teste-chave §6.5: sessão com purchase mas sem nada informativo pré-carrinho.
    df = _df(spark, [
        _ev("e1", "add_to_cart", "s-1", "10:00"),   # corte logo no início
        _ev("e2", "begin_checkout", "s-1", "10:01"),
        _ev("e3", "purchase", "s-1", "10:02"),
    ])
    linha = gm.montar_features(df).collect()[0]
    # nenhum evento elegível -> todas as contagens e flags zeradas
    assert linha.n_eventos_pre == 0
    assert linha.n_view_item_list == 0
    assert linha.n_select_item == 0
    assert linha.n_view_item == 0
    assert linha.n_click == 0
    assert linha.n_site_search == 0
    assert linha.n_file_download == 0
    assert linha.fez_lead_pre == 0
    assert linha.tem_select_item == 0
    assert linha.scroll_max == 0
    assert linha.video_percent_max == 0


def test_features_sem_add_to_cart_usa_todos_pre_checkout(spark):
    df = _df(spark, [
        _ev("e1", "page_view", "s-1", "10:00"),
        _ev("e2", "scroll", "s-1", "10:01", percent_scrolled=75),
        _ev("e3", "view_item_list", "s-1", "10:02"),
        _ev("e4", "select_item", "s-1", "10:03", item_id="PLANO-PRO"),
        _ev("e5", "generate_lead", "s-1", "10:04"),
        _ev("e6", "site_search", "s-1", "10:05"),
    ])
    linha = gm.montar_features(df).collect()[0]
    assert linha.n_eventos_pre == 6
    assert linha.scroll_max == 75
    assert linha.n_view_item_list == 1
    assert linha.n_select_item == 1
    assert linha.n_site_search == 1
    assert linha.fez_lead_pre == 1
    assert linha.tem_select_item == 1


def test_features_sessao_sem_eventos_elegiveis_ainda_presente(spark):
    # sessão cujo único evento é add_to_cart -> sem features, mas deve aparecer.
    df = _df(spark, [
        _ev("e1", "add_to_cart", "s-vazia", "10:00"),
        _ev("e2", "page_view", "s-cheia", "11:00", user_pseudo_id="anon-2"),
    ])
    por_id = {l.session_id: l for l in gm.montar_features(df).collect()}
    assert "s-vazia" in por_id
    assert por_id["s-vazia"].n_eventos_pre == 0
    assert por_id["s-vazia"].canal == "google / cpc"  # atributos ainda resolvidos


# --------------------------------------------------------------------------- #
# §6.3 — Rótulo (rotular)
# --------------------------------------------------------------------------- #

def test_rotulo_sessao_com_purchase(spark):
    df = _df(spark, [
        _ev("e1", "page_view", "s-compra", "10:00"),
        _ev("e2", "purchase", "s-compra", "10:10"),
        _ev("e3", "page_view", "s-sem", "11:00", user_pseudo_id="anon-2"),
    ])
    por_id = {l.session_id: l.label for l in gm.rotular(df).collect()}
    assert por_id["s-compra"] == 1
    assert por_id["s-sem"] == 0


# --------------------------------------------------------------------------- #
# §6.3/§6.4 — Treino, split e propensao_sessao
# --------------------------------------------------------------------------- #

def _dataset_separavel(spark):
    """Dataset sintético com separação óbvia: sessões 'quentes' (muito
    engajamento pré-carrinho) compram; sessões 'frias' (quase nada) não compram."""
    linhas = []
    # 12 sessões quentes que compram
    for i in range(12):
        s = f"s-quente-{i}"
        u = f"hot-{i}"
        linhas += [
            _ev(f"{s}-a", "page_view", s, "10:00", user_pseudo_id=u),
            _ev(f"{s}-b", "view_item_list", s, "10:01", user_pseudo_id=u),
            _ev(f"{s}-c", "select_item", s, "10:02", user_pseudo_id=u, item_id="PLANO-PRO"),
            _ev(f"{s}-d", "view_item", s, "10:03", user_pseudo_id=u),
            _ev(f"{s}-e", "scroll", s, "10:04", user_pseudo_id=u, percent_scrolled=90),
            _ev(f"{s}-f", "add_to_cart", s, "10:05", user_pseudo_id=u),
            _ev(f"{s}-g", "purchase", s, "10:10", user_pseudo_id=u),
        ]
    # 12 sessões frias que não compram
    for i in range(12):
        s = f"s-fria-{i}"
        u = f"cold-{i}"
        linhas += [
            _ev(f"{s}-a", "page_view", s, "10:00", user_pseudo_id=u),
        ]
    return _df(spark, linhas)


def test_propensao_cobre_todas_as_sessoes_e_prob_em_0_1(spark):
    df = _dataset_separavel(spark)
    features = gm.montar_features(df)
    rotulos = gm.rotular(df)
    fr = features.join(rotulos, "session_id", "left").withColumn(
        "label", F.coalesce(F.col("label"), F.lit(0)).cast("int")
    )
    propensao, _ = gm.treinar_e_escorar(fr)

    sessoes_silver = {l.session_id for l in df.select("session_id").distinct().collect()}
    sessoes_prop = {l.session_id for l in propensao.select("session_id").collect()}
    assert sessoes_prop == sessoes_silver
    assert propensao.count() == len(sessoes_silver)

    for l in propensao.collect():
        assert 0.0 <= l.probabilidade_compra <= 1.0
        assert l.conjunto in ("treino", "teste")
        assert l.rotulo in (0, 1)


def test_propensao_colunas_exatas(spark):
    df = _dataset_separavel(spark)
    features = gm.montar_features(df)
    rotulos = gm.rotular(df)
    fr = features.join(rotulos, "session_id", "left").withColumn(
        "label", F.coalesce(F.col("label"), F.lit(0)).cast("int")
    )
    propensao, _ = gm.treinar_e_escorar(fr)
    assert propensao.columns == ["session_id", "probabilidade_compra", "rotulo", "conjunto"]


def test_split_deterministico_seed_42(spark):
    df = _dataset_separavel(spark)
    features = gm.montar_features(df)
    rotulos = gm.rotular(df)
    fr = features.join(rotulos, "session_id", "left").withColumn(
        "label", F.coalesce(F.col("label"), F.lit(0)).cast("int")
    )
    p1, _ = gm.treinar_e_escorar(fr)
    p2, _ = gm.treinar_e_escorar(fr)
    teste1 = sorted(l.session_id for l in p1.where(F.col("conjunto") == "teste").collect())
    teste2 = sorted(l.session_id for l in p2.where(F.col("conjunto") == "teste").collect())
    assert teste1 == teste2  # mesma seed -> mesma partição treino/teste


# --------------------------------------------------------------------------- #
# §6.4 — metricas_modelo (EXATAMENTE 4 linhas) — apontamento 3
# --------------------------------------------------------------------------- #

def test_metricas_modelo_exatamente_4_linhas(spark):
    metricas = {
        "auc": 0.9, "f1": 0.8, "precisao": 0.7, "recall": 0.75,
        "n_teste": 10, "positivos_teste": 3,
    }
    tabela = gm.construir_metricas_modelo(spark, metricas)
    assert tabela.columns == ["metrica", "valor", "features"]
    assert tabela.count() == 4
    metricas_set = {l.metrica for l in tabela.collect()}
    assert metricas_set == {"auc", "f1", "precisao", "recall"}
    # n_teste / positivos_teste NÃO são linhas da tabela
    assert "n_teste" not in metricas_set
    assert "positivos_teste" not in metricas_set
    # features autocontida: lista exata em todas as linhas
    for l in tabela.collect():
        assert l.features == gm.LISTA_FEATURES


def test_treino_produz_4_metricas_reais(spark):
    df = _dataset_separavel(spark)
    features = gm.montar_features(df)
    rotulos = gm.rotular(df)
    fr = features.join(rotulos, "session_id", "left").withColumn(
        "label", F.coalesce(F.col("label"), F.lit(0)).cast("int")
    )
    _, metricas = gm.treinar_e_escorar(fr)
    for chave in ("auc", "f1", "precisao", "recall"):
        assert chave in metricas
        assert metricas[chave] is not None
    tabela = gm.construir_metricas_modelo(spark, metricas)
    assert tabela.count() == 4


# --------------------------------------------------------------------------- #
# §6.4 — Segmentação de visitantes (K-Means)
# --------------------------------------------------------------------------- #

def test_segmentacao_um_segmento_por_visitante_e_perfil(spark):
    df = _dataset_separavel(spark)
    fv = gm.montar_features_visitante(df)
    segmentos, perfil, k, sil = gm.segmentar_visitantes(fv)

    # um segmento por visitante
    visitantes = {l.user_pseudo_id for l in fv.collect()}
    seg_visitantes = [l.user_pseudo_id for l in segmentos.collect()]
    assert sorted(seg_visitantes) == sorted(visitantes)
    assert len(seg_visitantes) == len(set(seg_visitantes))  # sem duplicar
    assert segmentos.columns == ["user_pseudo_id", "segmento"]

    # perfil médio por grupo
    assert "segmento" in perfil.columns
    assert "n_visitantes" in perfil.columns
    for c in gm.FEATURES_VISITANTE:
        assert c in perfil.columns
    # soma de n_visitantes = total de visitantes
    assert sum(l.n_visitantes for l in perfil.collect()) == len(visitantes)


def test_features_visitante_uma_linha_por_visitante(spark):
    df = _df(spark, [
        _ev("e1", "page_view", "s-1", "10:00", user_pseudo_id="u1"),
        _ev("e2", "purchase", "s-1", "10:05", user_pseudo_id="u1"),
        _ev("e3", "page_view", "s-2", "12:00", user_pseudo_id="u1"),  # 2ª sessão
        _ev("e4", "page_view", "s-3", "10:00", user_pseudo_id="u2"),
    ])
    fv = gm.montar_features_visitante(df)
    por_u = {l.user_pseudo_id: l for l in fv.collect()}
    assert set(por_u) == {"u1", "u2"}
    assert por_u["u1"].n_sessoes == 2.0
    assert por_u["u1"].comprou_alguma_vez == 1.0
    assert por_u["u2"].comprou_alguma_vez == 0.0
