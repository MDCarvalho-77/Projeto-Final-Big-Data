"""Testes do job Gold do funil (pipeline/gold_funil.py) e do helper gold_comum.

Seguem o padrão de tests/pipeline/test_silver.py: usam a fixture `spark`
(local[2]) e chamam as funções puras diretamente sobre DataFrames em memória com
as colunas tipadas da Silver (event_name, event_ts_ts, event_id, session_id,
traffic/device structs, item_id, pagina, percent_scrolled). Não tocam o S3.

Cobrem (FEAT-001):
- contagem por etapa = sessões distintas que tiveram o evento;
- view_item fora da sequência, presente só como taxa_view_item;
- taxa_view_item POR ESCOPO (igual em todas as linhas do total; DIFERENTE por par
  no segmentado quando os dados divergem — prova explícita de que a segmentada !=
  global);
- conv_etapa_anterior / conv_topo;
- try_divide -> null em recorte com denominador 0;
- funil_lead invariante sessoes_lead <= sessoes_scroll_50 <= sessoes_page_view e
  filtro do sentinela '(total)';
- gold_comum.atributos_sessao (canal com coalesce, plano, dispositivo) e to_date_utc.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import gold_comum
import gold_funil


# --------------------------------------------------------------------------- #
# Helpers: montar eventos no formato da Silver
# --------------------------------------------------------------------------- #

_TRAFFIC = StructType([
    StructField("source", StringType()),
    StructField("medium", StringType()),
    StructField("campaign", StringType()),
])
_DEVICE = StructType([
    StructField("category", StringType()),
    StructField("os", StringType()),
    StructField("browser", StringType()),
])

_SCHEMA = StructType([
    StructField("session_id", StringType()),
    StructField("event_id", StringType()),
    StructField("event_name", StringType()),
    StructField("event_ts_ts", TimestampType()),
    StructField("traffic", _TRAFFIC),
    StructField("device", _DEVICE),
    StructField("item_id", StringType()),
    StructField("pagina", StringType()),
    StructField("percent_scrolled", IntegerType()),
])


def _ts(minuto):
    """Timestamp de referência: _ts(5) -> 2026-09-01 10:05:00 UTC."""
    return dt.datetime(2026, 9, 1, 10, minuto, 0)


def _ev(session_id, event_id, event_name, minuto=0, source="google", medium="cpc",
        category="mobile", item_id=None, pagina="/", percent_scrolled=None):
    traffic = Row(source=source, medium=medium, campaign="c")
    device = Row(category=category, os="Android", browser="Chrome")
    return Row(
        session_id=session_id,
        event_id=event_id,
        event_name=event_name,
        event_ts_ts=_ts(minuto),
        traffic=traffic,
        device=device,
        item_id=item_id,
        pagina=pagina,
        percent_scrolled=percent_scrolled,
    )


def _df(spark, linhas):
    return spark.createDataFrame(linhas, schema=_SCHEMA)


def _por_etapa(funil_compra, escopo):
    """Dict etapa -> linha (Row) do escopo total ou segmentado agregado."""
    linhas = funil_compra.where(F.col("escopo") == escopo).collect()
    return {l.etapa: l for l in linhas}


# --------------------------------------------------------------------------- #
# gold_comum.atributos_sessao
# --------------------------------------------------------------------------- #

def test_atributos_canal_plano_dispositivo(spark):
    eventos = _df(spark, [
        _ev("s1", "e1", "page_view", minuto=0, source="google", medium="cpc", category="mobile"),
        _ev("s1", "e2", "select_item", minuto=1, item_id="PLANO-PRO"),
        _ev("s1", "e3", "select_item", minuto=2, item_id="PLANO-BASICO"),
    ])
    linha = gold_comum.atributos_sessao(eventos).collect()[0]
    assert linha.canal == "google / cpc"
    assert linha.dispositivo == "mobile"
    # plano = item_id do PRIMEIRO select_item
    assert linha.plano == "PLANO-PRO"


def test_atributos_sem_select_item_plano_nenhum(spark):
    eventos = _df(spark, [
        _ev("s1", "e1", "page_view", minuto=0),
    ])
    linha = gold_comum.atributos_sessao(eventos).collect()[0]
    assert linha.plano == gold_comum.PLANO_NENHUM


def test_atributos_canal_coalesce_antes_do_concat(spark):
    # source e medium nulos: separador " / " deve permanecer (coalesce -> "").
    eventos = _df(spark, [
        _ev("s1", "e1", "page_view", minuto=0, source=None, medium=None),
        _ev("s2", "e2", "page_view", minuto=0, source=None, medium="cpc"),
        _ev("s3", "e3", "page_view", minuto=0, source="google", medium=None),
    ])
    canais = {l.session_id: l.canal for l in gold_comum.atributos_sessao(eventos).collect()}
    assert canais["s1"] == " / "
    assert canais["s2"] == " / cpc"
    assert canais["s3"] == "google / "


def test_atributos_primeiro_evento_define_canal(spark):
    # o canal vem do PRIMEIRO evento (menor event_ts_ts), não de eventos seguintes
    eventos = _df(spark, [
        _ev("s1", "e2", "scroll", minuto=5, source="facebook", medium="social"),
        _ev("s1", "e1", "page_view", minuto=0, source="google", medium="cpc"),
    ])
    linha = gold_comum.atributos_sessao(eventos).collect()[0]
    assert linha.canal == "google / cpc"


def test_to_date_utc(spark):
    eventos = _df(spark, [_ev("s1", "e1", "purchase", minuto=0)])
    linha = eventos.withColumn("d", gold_comum.to_date_utc("event_ts_ts")).collect()[0]
    assert linha.d == dt.date(2026, 9, 1)


# --------------------------------------------------------------------------- #
# funil_compra — contagem por etapa
# --------------------------------------------------------------------------- #

def test_funil_compra_contagem_por_etapa_sessoes_distintas(spark):
    # s1: percorre view_item_list -> select_item -> add_to_cart
    # s2: só view_item_list
    eventos = _df(spark, [
        _ev("s1", "e1", "view_item_list", minuto=0),
        _ev("s1", "e2", "select_item", minuto=1, item_id="PLANO-PRO"),
        _ev("s1", "e3", "select_item", minuto=2, item_id="PLANO-PRO"),  # 2 eventos, 1 sessão
        _ev("s1", "e4", "add_to_cart", minuto=3),
        _ev("s2", "e5", "view_item_list", minuto=0),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compra = gold_funil.construir_funil_compra(eventos, atributos)
    total = _por_etapa(compra, "total")
    # view_item_list: 2 sessões distintas; select_item: 1; add_to_cart: 1
    assert total["view_item_list"].sessoes == 2
    assert total["select_item"].sessoes == 1
    assert total["add_to_cart"].sessoes == 1
    # etapas não atingidas = 0
    assert total["purchase"].sessoes == 0
    # ordem_etapa 1..7
    assert total["view_item_list"].ordem_etapa == 1
    assert total["purchase"].ordem_etapa == 7


def test_funil_compra_view_item_fora_da_sequencia(spark):
    # view_item não é etapa: não aparece como linha, só como taxa_view_item
    eventos = _df(spark, [
        _ev("s1", "e1", "view_item", minuto=0),
        _ev("s1", "e2", "select_item", minuto=1, item_id="PLANO-PRO"),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compra = gold_funil.construir_funil_compra(eventos, atributos)
    etapas = {l.etapa for l in compra.collect()}
    assert "view_item" not in etapas
    assert etapas == set(gold_funil.ETAPAS_COMPRA)


def test_funil_compra_taxa_view_item_total(spark):
    # 2 sessões com view_item, 1 com select_item -> taxa global = 2/1 = 2.0
    eventos = _df(spark, [
        _ev("s1", "e1", "view_item", minuto=0),
        _ev("s1", "e2", "select_item", minuto=1, item_id="PLANO-PRO"),
        _ev("s2", "e3", "view_item", minuto=0),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compra = gold_funil.construir_funil_compra(eventos, atributos)
    total = _por_etapa(compra, "total")
    taxas = {l.taxa_view_item for l in compra.where(F.col("escopo") == "total").collect()}
    # a taxa é a mesma em todas as 7 linhas do escopo total
    assert taxas == {2.0}
    assert total["purchase"].taxa_view_item == 2.0


def test_funil_compra_taxa_view_item_segmentada_difere_da_global(spark):
    """Prova explícita: taxa_view_item no segmentado usa o par plano×canal da
    linha, NÃO a taxa global."""
    # par A (PLANO-PRO / google/cpc): 2 sessões com view_item, 2 com select_item -> 1.0
    # par B (PLANO-BASICO / facebook/social): 1 view_item, 0 select_item... usamos
    #   um cenário em que a global difere de cada par.
    eventos = _df(spark, [
        # sessão sa: PRO, google/cpc, tem view_item e select_item
        _ev("sa", "a1", "view_item", minuto=0, source="google", medium="cpc"),
        _ev("sa", "a2", "select_item", minuto=1, source="google", medium="cpc", item_id="PLANO-PRO"),
        # sessão sb: PRO, google/cpc, tem view_item e select_item
        _ev("sb", "b1", "view_item", minuto=0, source="google", medium="cpc"),
        _ev("sb", "b2", "select_item", minuto=1, source="google", medium="cpc", item_id="PLANO-PRO"),
        # sessão sc: BASICO, facebook/social, tem view_item mas sem select_item
        _ev("sc", "c1", "view_item", minuto=0, source="facebook", medium="social"),
        _ev("sc", "c2", "add_to_cart", minuto=1, source="facebook", medium="social"),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compra = gold_funil.construir_funil_compra(eventos, atributos)

    # global: view_item=3 sessões, select_item=2 sessões -> 1.5
    total_taxa = {l.taxa_view_item for l in compra.where(F.col("escopo") == "total").collect()}
    assert total_taxa == {1.5}

    seg = compra.where(F.col("escopo") == "segmentado").collect()
    por_par = {(l.plano, l.canal): l.taxa_view_item for l in seg}
    # par A: 2 view_item / 2 select_item = 1.0 (DIFERENTE da global 1.5)
    assert por_par[("PLANO-PRO", "google / cpc")] == 1.0
    # par C: canal facebook/social, plano "(nenhum)" (sc não teve select_item):
    #   1 view_item / 0 select_item -> try_divide -> None
    assert por_par[(gold_comum.PLANO_NENHUM, "facebook / social")] is None
    # confirma que a segmentada != global
    assert 1.0 != 1.5


def test_funil_compra_conversoes_anterior_e_topo(spark):
    # topo (view_item_list) = 2 sessões; select_item = 1; add_to_cart = 1
    eventos = _df(spark, [
        _ev("s1", "e1", "view_item_list", minuto=0),
        _ev("s1", "e2", "select_item", minuto=1, item_id="PLANO-PRO"),
        _ev("s1", "e3", "add_to_cart", minuto=2),
        _ev("s2", "e4", "view_item_list", minuto=0),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compra = gold_funil.construir_funil_compra(eventos, atributos)
    total = _por_etapa(compra, "total")
    # 1ª etapa: conv_etapa_anterior = None, conv_topo = 1.0
    assert total["view_item_list"].conv_etapa_anterior is None
    assert total["view_item_list"].conv_topo == 1.0
    # select_item: anterior = 1/2 = 0.5; topo = 1/2 = 0.5
    assert total["select_item"].conv_etapa_anterior == 0.5
    assert total["select_item"].conv_topo == 0.5
    # add_to_cart: anterior = 1/1 = 1.0; topo = 1/2 = 0.5
    assert total["add_to_cart"].conv_etapa_anterior == 1.0
    assert total["add_to_cart"].conv_topo == 0.5


def test_funil_compra_try_divide_denominador_zero(spark):
    # recorte em que o topo (view_item_list) é 0 mas há etapas posteriores:
    # conv_topo e conv_etapa_anterior usam try_divide -> None
    eventos = _df(spark, [
        _ev("s1", "e1", "purchase", minuto=0),  # só purchase, nenhuma etapa anterior
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compra = gold_funil.construir_funil_compra(eventos, atributos)
    total = _por_etapa(compra, "total")
    # view_item_list = 0 sessões; purchase = 1
    assert total["view_item_list"].sessoes == 0
    assert total["purchase"].sessoes == 1
    # conv_topo de purchase = 1 / 0 -> None (try_divide)
    assert total["purchase"].conv_topo is None
    # conv_etapa_anterior de purchase = 1 / 0 (add_payment_info=0) -> None
    assert total["purchase"].conv_etapa_anterior is None
    # taxa_view_item sem select_item -> None
    assert total["purchase"].taxa_view_item is None


# --------------------------------------------------------------------------- #
# funil_lead
# --------------------------------------------------------------------------- #

def test_funil_lead_invariante_e_conversoes(spark):
    # s1: page_view landing + scroll 50 + lead
    # s2: page_view landing + scroll 50 (sem lead)
    # s3: page_view landing (sem scroll)
    eventos = _df(spark, [
        _ev("s1", "e1", "page_view", minuto=0, pagina="/"),
        _ev("s1", "e2", "scroll", minuto=1, pagina="/", percent_scrolled=75),
        _ev("s1", "e3", "generate_lead", minuto=2, pagina="/"),
        _ev("s2", "e4", "page_view", minuto=0, pagina="/"),
        _ev("s2", "e5", "scroll", minuto=1, pagina="/", percent_scrolled=50),
        _ev("s3", "e6", "page_view", minuto=0, pagina="/"),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    lead = gold_funil.construir_funil_lead(eventos, atributos)
    total = lead.where(F.col("canal") == gold_funil.TOTAL_LEAD).collect()[0]
    assert total.sessoes_page_view == 3
    assert total.sessoes_scroll_50 == 2
    assert total.sessoes_lead == 1
    # invariante lead <= scroll_50 <= page_view
    assert total.sessoes_lead <= total.sessoes_scroll_50 <= total.sessoes_page_view
    # conv_scroll = 2/3; conv_lead = 1/2
    assert abs(total.conv_scroll - 2 / 3) < 1e-9
    assert total.conv_lead == 0.5


def test_funil_lead_scroll_abaixo_de_50_nao_conta(spark):
    eventos = _df(spark, [
        _ev("s1", "e1", "page_view", minuto=0, pagina="/"),
        _ev("s1", "e2", "scroll", minuto=1, pagina="/", percent_scrolled=25),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    lead = gold_funil.construir_funil_lead(eventos, atributos)
    total = lead.where(F.col("canal") == gold_funil.TOTAL_LEAD).collect()[0]
    assert total.sessoes_page_view == 1
    assert total.sessoes_scroll_50 == 0


def test_funil_lead_scroll_fora_da_landing_nao_conta(spark):
    eventos = _df(spark, [
        _ev("s1", "e1", "page_view", minuto=0, pagina="/"),
        _ev("s1", "e2", "scroll", minuto=1, pagina="/checkout", percent_scrolled=90),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    lead = gold_funil.construir_funil_lead(eventos, atributos)
    total = lead.where(F.col("canal") == gold_funil.TOTAL_LEAD).collect()[0]
    assert total.sessoes_scroll_50 == 0


def test_funil_lead_sentinela_total_separado_dos_canais(spark):
    eventos = _df(spark, [
        _ev("s1", "e1", "page_view", minuto=0, pagina="/", source="google", medium="cpc"),
        _ev("s2", "e2", "page_view", minuto=0, pagina="/", source="facebook", medium="social"),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    lead = gold_funil.construir_funil_lead(eventos, atributos)
    # linha-sentinela existe e separa o total
    total = lead.where(F.col("canal") == gold_funil.TOTAL_LEAD).collect()[0]
    assert total.sessoes_page_view == 2
    # soma por canal (filtrando o sentinela) == total
    canais = lead.where(F.col("canal") != gold_funil.TOTAL_LEAD)
    soma = canais.agg(F.sum("sessoes_page_view").alias("s")).collect()[0].s
    assert soma == 2
    # cada canal tem 1 sessão
    por_canal = {l.canal: l.sessoes_page_view for l in canais.collect()}
    assert por_canal == {"google / cpc": 1, "facebook / social": 1}
