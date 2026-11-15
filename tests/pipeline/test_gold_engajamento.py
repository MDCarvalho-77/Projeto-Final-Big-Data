"""Testes do job Gold de engajamento (pipeline/gold_engajamento.py, FEAT-002).

Seguem o padrão de tests/pipeline/test_silver.py: usam a fixture `spark`
(local[2]) e exercitam as funções puras (`construir_sessoes`,
`construir_retencao_video`, `construir_buscas_faq`, `construir_saidas_downloads`)
com DataFrames em memória no schema que `ler_eventos(spark, "silver")` entrega —
isto é, com as colunas tipadas de params promovidas (percent_scrolled,
video_percent, outbound, link_domain, file_name, search_term, search_results),
`event_ts_ts` já como timestamp, `session_id`/`user_pseudo_id`/
`user_id_resolvido`, e os structs `traffic`/`device` usados por `atributos_sessao`.

Cobrem:
- sessoes: uma linha por sessão; comprou/fez_lead corretos; duracao_seg >= 0;
- retencao_video: denominador = sessões com video_start; marcos {10,25,50,75,100};
  pct_retencao monotônico decrescente;
- buscas_faq: buscas_sem_resultado só com search_results=0 e <= buscas;
- saidas_downloads: os três casos do apontamento 4 de build-requirements
  (domínio informado vira saída; link_domain nulo descarta a saída; download
  legítimo preservado mesmo sem link_domain).
"""

from __future__ import annotations

from datetime import datetime

import pytest

import gold_engajamento as ge
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
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
    StructField("outbound", BooleanType()),
    StructField("link_domain", StringType()),
    StructField("file_name", StringType()),
    StructField("search_term", StringType()),
    StructField("search_results", IntegerType()),
    StructField("item_id", StringType()),
    StructField("traffic", _TRAFFIC),
    StructField("device", _DEVICE),
])


def _ts(minuto_segundo: str) -> datetime:
    """'10:00' -> 2026-09-01 10:00:00 (UTC-naive, suficiente para deltas)."""
    return datetime.fromisoformat(f"2026-09-01T{minuto_segundo}:00")


def _ev(event_id, event_name, session_id, event_ts="10:00",
        user_pseudo_id="anon-1", user_id_resolvido=None,
        percent_scrolled=None, video_percent=None, outbound=None,
        link_domain=None, file_name=None, search_term=None, search_results=None,
        item_id=None, source="google", medium="cpc", category="mobile"):
    return (
        event_id, event_name, _ts(event_ts), session_id,
        user_pseudo_id, user_id_resolvido,
        percent_scrolled, video_percent, outbound,
        link_domain, file_name, search_term, search_results, item_id,
        (source, medium), (category,),
    )


def _df(spark, linhas):
    return spark.createDataFrame(linhas, SCHEMA_EVENTOS)


# --------------------------------------------------------------------------- #
# sessoes (§4.1)
# --------------------------------------------------------------------------- #

def test_sessoes_uma_linha_por_sessao(spark):
    df = _df(spark, [
        _ev("e1", "page_view", "s-1", "10:00"),
        _ev("e2", "scroll", "s-1", "10:05", percent_scrolled=50),
        _ev("e3", "page_view", "s-2", "11:00", user_pseudo_id="anon-2"),
    ])
    sessoes = ge.construir_sessoes(df)
    assert sessoes.count() == 2
    assert {l.session_id for l in sessoes.collect()} == {"s-1", "s-2"}


def test_sessoes_colunas_exatas(spark):
    df = _df(spark, [_ev("e1", "page_view", "s-1")])
    sessoes = ge.construir_sessoes(df)
    assert sessoes.columns == [
        "session_id", "user_pseudo_id", "user_id_resolvido", "canal",
        "dispositivo", "inicio", "fim", "duracao_seg", "n_eventos",
        "scroll_max", "video_percent_max", "fez_lead", "comprou",
    ]


def test_sessoes_comprou_e_fez_lead(spark):
    df = _df(spark, [
        # s-compra: tem purchase e generate_lead
        _ev("e1", "page_view", "s-compra", "10:00"),
        _ev("e2", "generate_lead", "s-compra", "10:05"),
        _ev("e3", "purchase", "s-compra", "10:10"),
        # s-so-lead: só lead
        _ev("e4", "generate_lead", "s-so-lead", "10:00", user_pseudo_id="anon-2"),
        # s-nada: nenhum dos dois
        _ev("e5", "page_view", "s-nada", "10:00", user_pseudo_id="anon-3"),
    ])
    por_id = {l.session_id: l for l in ge.construir_sessoes(df).collect()}
    assert por_id["s-compra"].comprou is True
    assert por_id["s-compra"].fez_lead is True
    assert por_id["s-so-lead"].comprou is False
    assert por_id["s-so-lead"].fez_lead is True
    assert por_id["s-nada"].comprou is False
    assert por_id["s-nada"].fez_lead is False


def test_sessoes_duracao_nao_negativa_e_coerente(spark):
    df = _df(spark, [
        _ev("e1", "page_view", "s-1", "10:00"),
        _ev("e2", "scroll", "s-1", "10:02", percent_scrolled=25),
        _ev("e3", "page_view", "s-1", "10:01"),
    ])
    linha = ge.construir_sessoes(df).collect()[0]
    assert linha.duracao_seg == 120  # 10:02 - 10:00
    assert linha.duracao_seg >= 0
    assert linha.n_eventos == 3


def test_sessoes_canal_e_dispositivo(spark):
    df = _df(spark, [
        _ev("e1", "page_view", "s-1", "10:00", source="google", medium="cpc", category="mobile"),
    ])
    linha = ge.construir_sessoes(df).collect()[0]
    assert linha.canal == "google / cpc"
    assert linha.dispositivo == "mobile"


def test_sessoes_scroll_e_video_max(spark):
    df = _df(spark, [
        _ev("e1", "scroll", "s-1", "10:00", percent_scrolled=25),
        _ev("e2", "scroll", "s-1", "10:05", percent_scrolled=75),
        _ev("e3", "video_progress", "s-1", "10:06", video_percent=50),
        _ev("e4", "video_progress", "s-1", "10:07", video_percent=100),
    ])
    linha = ge.construir_sessoes(df).collect()[0]
    assert linha.scroll_max == 75
    assert linha.video_percent_max == 100


# --------------------------------------------------------------------------- #
# retencao_video (§4.2)
# --------------------------------------------------------------------------- #

def test_retencao_colunas_e_marcos(spark):
    df = _df(spark, [
        _ev("e1", "video_start", "s-1", "10:00", video_percent=0),
        _ev("e2", "video_progress", "s-1", "10:01", video_percent=50),
    ])
    retencao = ge.construir_retencao_video(df)
    assert retencao.columns == ["marco", "sessoes_no_marco", "pct_retencao"]
    assert sorted(l.marco for l in retencao.collect()) == [10, 25, 50, 75, 100]


def test_retencao_denominador_so_sessoes_com_video_start(spark):
    df = _df(spark, [
        # s-start: tem video_start e chega a 100
        _ev("e1", "video_start", "s-start", "10:00", video_percent=0),
        _ev("e2", "video_complete", "s-start", "10:05", video_percent=100),
        # s-sem-start: eventos de vídeo SEM video_start -> não entra no denominador
        _ev("e3", "video_progress", "s-sem-start", "10:00", video_percent=75, user_pseudo_id="anon-2"),
    ])
    por_marco = {l.marco: l for l in ge.construir_retencao_video(df).collect()}
    # denominador = 1 (só s-start); s-sem-start é ignorada mesmo chegando a 75
    assert por_marco[10].sessoes_no_marco == 1
    assert por_marco[100].sessoes_no_marco == 1
    assert por_marco[100].pct_retencao == 1.0


def test_retencao_pct_monotonico_decrescente(spark):
    df = _df(spark, [
        # 3 sessões com video_start, atingindo 100, 50 e 10 respectivamente
        _ev("a1", "video_start", "s-100", "10:00", video_percent=0),
        _ev("a2", "video_complete", "s-100", "10:05", video_percent=100),
        _ev("b1", "video_start", "s-50", "10:00", video_percent=0, user_pseudo_id="anon-2"),
        _ev("b2", "video_progress", "s-50", "10:05", video_percent=50, user_pseudo_id="anon-2"),
        _ev("c1", "video_start", "s-10", "10:00", video_percent=0, user_pseudo_id="anon-3"),
        _ev("c2", "video_progress", "s-10", "10:05", video_percent=10, user_pseudo_id="anon-3"),
    ])
    por_marco = {l.marco: l for l in ge.construir_retencao_video(df).collect()}
    # 10%: as 3; 25/50: 2 (s-100, s-50); 75/100: 1 (s-100)
    assert por_marco[10].sessoes_no_marco == 3
    assert por_marco[25].sessoes_no_marco == 2
    assert por_marco[50].sessoes_no_marco == 2
    assert por_marco[75].sessoes_no_marco == 1
    assert por_marco[100].sessoes_no_marco == 1
    seq = [por_marco[m].pct_retencao for m in [10, 25, 50, 75, 100]]
    assert all(a >= b for a, b in zip(seq, seq[1:]))  # monotônico decrescente


def test_retencao_sem_video_start_todas_linhas_zero(spark):
    df = _df(spark, [_ev("e1", "page_view", "s-1", "10:00")])
    retencao = ge.construir_retencao_video(df)
    assert retencao.count() == 5
    for l in retencao.collect():
        assert l.sessoes_no_marco == 0
        assert l.pct_retencao is None  # try_divide por 0 -> null


# --------------------------------------------------------------------------- #
# buscas_faq (§4.3)
# --------------------------------------------------------------------------- #

def test_buscas_colunas_e_contagem(spark):
    df = _df(spark, [
        _ev("e1", "site_search", "s-1", search_term="reembolso", search_results=3),
        _ev("e2", "site_search", "s-1", search_term="reembolso", search_results=0),
        _ev("e3", "site_search", "s-2", search_term="certificado", search_results=5, user_pseudo_id="anon-2"),
        _ev("e4", "page_view", "s-1"),  # não-busca, ignorado
    ])
    por_termo = {l.search_term: l for l in ge.construir_buscas_faq(df).collect()}
    assert ge.construir_buscas_faq(df).columns == ["search_term", "buscas", "buscas_sem_resultado"]
    assert por_termo["reembolso"].buscas == 2
    assert por_termo["reembolso"].buscas_sem_resultado == 1
    assert por_termo["certificado"].buscas == 1
    assert por_termo["certificado"].buscas_sem_resultado == 0


def test_buscas_sem_resultado_nunca_maior_que_buscas(spark):
    df = _df(spark, [
        _ev("e1", "site_search", "s-1", search_term="x", search_results=0),
        _ev("e2", "site_search", "s-1", search_term="x", search_results=0),
        _ev("e3", "site_search", "s-2", search_term="y", search_results=2, user_pseudo_id="anon-2"),
    ])
    for l in ge.construir_buscas_faq(df).collect():
        assert l.buscas_sem_resultado <= l.buscas


def test_buscas_termo_nulo_mantido_como_linha(spark):
    df = _df(spark, [
        _ev("e1", "site_search", "s-1", search_term=None, search_results=0),
    ])
    linhas = ge.construir_buscas_faq(df).collect()
    assert len(linhas) == 1
    assert linhas[0].search_term is None
    assert linhas[0].buscas == 1
    assert linhas[0].buscas_sem_resultado == 1


# --------------------------------------------------------------------------- #
# saidas_downloads (§4.4 + apontamento 4)
# --------------------------------------------------------------------------- #

def test_saidas_downloads_colunas(spark):
    df = _df(spark, [
        _ev("e1", "click", "s-1", outbound=True, link_domain="youtube.com"),
    ])
    assert ge.construir_saidas_downloads(df).columns == ["tipo", "chave", "ocorrencias", "sessoes"]


def test_saida_com_dominio_informado_vira_saida(spark):
    df = _df(spark, [
        _ev("e1", "click", "s-1", outbound=True, link_domain="youtube.com"),
        _ev("e2", "click", "s-2", outbound=True, link_domain="youtube.com", user_pseudo_id="anon-2"),
    ])
    saidas = [l for l in ge.construir_saidas_downloads(df).collect() if l.tipo == "saida"]
    assert len(saidas) == 1
    assert saidas[0].chave == "youtube.com"
    assert saidas[0].ocorrencias == 2
    assert saidas[0].sessoes == 2


def test_saida_com_link_domain_nulo_e_descartada(spark):
    df = _df(spark, [
        _ev("e1", "click", "s-1", outbound=True, link_domain=None),  # saída sem domínio
        _ev("e2", "click", "s-1", outbound=True, link_domain="instagram.com"),
    ])
    saidas = [l for l in ge.construir_saidas_downloads(df).collect() if l.tipo == "saida"]
    # só a de domínio informado sobrevive; a de link_domain nulo é descartada
    assert len(saidas) == 1
    assert saidas[0].chave == "instagram.com"
    assert all(l.chave is not None for l in saidas)


def test_download_preservado_mesmo_sem_link_domain(spark):
    df = _df(spark, [
        # download legítimo: file_download com file_name e SEM link_domain
        _ev("e1", "file_download", "s-1", file_name="apostila.pdf", link_domain=None),
        _ev("e2", "file_download", "s-2", file_name="apostila.pdf", link_domain=None, user_pseudo_id="anon-2"),
        # uma saída sem domínio não deve "levar junto" o download
        _ev("e3", "click", "s-1", outbound=True, link_domain=None),
    ])
    resultado = ge.construir_saidas_downloads(df).collect()
    downloads = [l for l in resultado if l.tipo == "download"]
    saidas = [l for l in resultado if l.tipo == "saida"]
    assert len(downloads) == 1
    assert downloads[0].chave == "apostila.pdf"
    assert downloads[0].ocorrencias == 2
    assert downloads[0].sessoes == 2
    assert saidas == []  # a única saída tinha link_domain nulo -> descartada


def test_click_nao_outbound_nao_vira_saida(spark):
    df = _df(spark, [
        _ev("e1", "click", "s-1", outbound=False, link_domain="interno.com"),
        _ev("e2", "click", "s-1", outbound=None, link_domain="interno.com"),
    ])
    saidas = [l for l in ge.construir_saidas_downloads(df).collect() if l.tipo == "saida"]
    assert saidas == []
