"""Testes do job Gold de receita e aquisição (pipeline/gold_receita_aquisicao.py).

Seguem o padrão de tests/pipeline/test_silver.py e test_gold_funil.py: usam a
fixture `spark` (local[2]) e chamam as funções puras diretamente sobre DataFrames
em memória com as colunas tipadas da Silver (eventos e silver/itens). Não tocam
o S3.

Cobrem (FEAT-003):
- receita bruta = Σ value dos purchase (NÃO soma itens);
- reembolso atribuído por transaction_id (refund→purchase) e lançado na DATA DA
  COMPRA;
- receita_liquida = bruta − reembolsada E receita_bruta >= receita_reembolsada
  POR LINHA (sem linha extra criada por refund, sem pedidos=0);
- reembolso com DIVERGÊNCIA de canal (canal do refund vem da compra);
- plano e order bump ligados por event_id (nunca por transaction_id);
- ticket_medio via try_divide;
- order bump por item_id='BUMP-MENTORIA';
- lead→compra por pessoa, 'estritamente depois', dias fracionário, mediana via
  percentile_approx.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import gold_comum
import gold_receita_aquisicao as gra


# --------------------------------------------------------------------------- #
# Helpers: montar eventos e itens no formato da Silver
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

_SCHEMA_EVENTOS = StructType([
    StructField("session_id", StringType()),
    StructField("event_id", StringType()),
    StructField("event_name", StringType()),
    StructField("event_ts_ts", TimestampType()),
    StructField("user_pseudo_id", StringType()),
    StructField("user_id_resolvido", StringType()),
    StructField("traffic", _TRAFFIC),
    StructField("device", _DEVICE),
    StructField("value", DoubleType()),
    StructField("transaction_id", StringType()),
    StructField("item_id", StringType()),
])

_SCHEMA_ITENS = StructType([
    StructField("event_id", StringType()),
    StructField("item_id", StringType()),
    StructField("item_category", StringType()),
    StructField("index", LongType()),
])


def _ts(minuto, dia=1, hora=10):
    return dt.datetime(2026, 9, dia, hora, minuto, 0)


def _ev(session_id, event_id, event_name, ts, user_pseudo_id="u1",
        user_id_resolvido=None, source="google", medium="cpc", category="mobile",
        value=None, transaction_id=None, item_id=None):
    traffic = Row(source=source, medium=medium, campaign="c")
    device = Row(category=category, os="Android", browser="Chrome")
    return Row(
        session_id=session_id,
        event_id=event_id,
        event_name=event_name,
        event_ts_ts=ts,
        user_pseudo_id=user_pseudo_id,
        user_id_resolvido=user_id_resolvido,
        traffic=traffic,
        device=device,
        value=value,
        transaction_id=transaction_id,
        item_id=item_id,
    )


def _item(event_id, item_id, item_category, index=0):
    return Row(event_id=event_id, item_id=item_id, item_category=item_category, index=index)


def _df_eventos(spark, linhas):
    return spark.createDataFrame(linhas, schema=_SCHEMA_EVENTOS)


def _df_itens(spark, linhas):
    return spark.createDataFrame(linhas, schema=_SCHEMA_ITENS)


# --------------------------------------------------------------------------- #
# receita_diaria — receita bruta = Σ value dos purchase (não soma itens)
# --------------------------------------------------------------------------- #

def test_receita_bruta_e_soma_de_value_dos_purchase(spark):
    # Um purchase com value=597 e DOIS itens (plano + bump): a bruta é 597,
    # não a soma dos itens.
    eventos = _df_eventos(spark, [
        _ev("s1", "p1", "purchase", _ts(0), value=597.0, transaction_id="T1"),
    ])
    itens = _df_itens(spark, [
        _item("p1", "PLANO-PRO", "plano", index=0),
        _item("p1", "BUMP-MENTORIA", "bump", index=1),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compras = gra.compras_enriquecidas(eventos, itens, atributos)
    reembolsos = gra.reembolsos_atribuidos(eventos, compras)
    receita = gra.construir_receita_diaria(compras, reembolsos).collect()
    assert len(receita) == 1
    linha = receita[0]
    assert linha.receita_bruta == 597.0
    assert linha.pedidos == 1
    assert linha.plano == "PLANO-PRO"  # plano ligado por event_id, não soma itens


def test_receita_liquida_e_invariante_bruta_maior_que_reembolsada(spark):
    # Compra 597 + reembolso 597 (total) na mesma linha (data da compra).
    eventos = _df_eventos(spark, [
        _ev("s1", "p1", "purchase", _ts(0, dia=1), value=597.0, transaction_id="T1"),
        # refund DOIS dias depois: deve ser lançado na data da COMPRA (dia 1).
        _ev("s2", "r1", "refund", _ts(0, dia=3), value=597.0, transaction_id="T1"),
    ])
    itens = _df_itens(spark, [_item("p1", "PLANO-PRO", "plano")])
    atributos = gold_comum.atributos_sessao(eventos)
    compras = gra.compras_enriquecidas(eventos, itens, atributos)
    reembolsos = gra.reembolsos_atribuidos(eventos, compras)
    receita = gra.construir_receita_diaria(compras, reembolsos).collect()

    # Uma única linha: nenhuma linha extra criada pelo refund.
    assert len(receita) == 1
    linha = receita[0]
    assert linha.data_compra == dt.date(2026, 9, 1)  # data da COMPRA, não do refund
    assert linha.receita_bruta == 597.0
    assert linha.receita_reembolsada == 597.0
    assert linha.receita_liquida == 0.0
    assert linha.pedidos == 1  # pedidos nunca 0 por causa de refund
    # invariante por linha
    assert linha.receita_bruta >= linha.receita_reembolsada


def test_reembolso_divergencia_de_canal_usa_canal_da_compra(spark):
    # A compra é no canal google/cpc; o evento refund está numa sessão de
    # canal DIFERENTE (facebook/social). O reembolso deve cair no canal da COMPRA.
    eventos = _df_eventos(spark, [
        _ev("s1", "p1", "purchase", _ts(0), source="google", medium="cpc",
            value=297.0, transaction_id="T1"),
        _ev("s2", "r1", "refund", _ts(5), source="facebook", medium="social",
            value=297.0, transaction_id="T1"),
    ])
    itens = _df_itens(spark, [_item("p1", "PLANO-BASICO", "plano")])
    atributos = gold_comum.atributos_sessao(eventos)
    compras = gra.compras_enriquecidas(eventos, itens, atributos)
    reembolsos = gra.reembolsos_atribuidos(eventos, compras)
    reembolsos_linhas = reembolsos.collect()
    assert len(reembolsos_linhas) == 1
    # canal herdado da COMPRA (google / cpc), não da sessão do refund
    assert reembolsos_linhas[0].canal == "google / cpc"

    receita = gra.construir_receita_diaria(compras, reembolsos).collect()
    # a reembolsada entra na linha google/cpc, não numa linha facebook/social
    linhas = {l.canal: l for l in receita}
    assert "facebook / social" not in linhas
    assert linhas["google / cpc"].receita_reembolsada == 297.0


def test_receita_nao_cria_linha_so_reembolso(spark):
    # Duas compras em canais diferentes; reembolso atribuído a uma delas.
    # O FULL OUTER criaria risco de linha só-refund — garantimos que o LEFT JOIN
    # mantém só as linhas com pedidos>=1.
    eventos = _df_eventos(spark, [
        _ev("s1", "p1", "purchase", _ts(0), source="google", medium="cpc",
            value=297.0, transaction_id="T1"),
        _ev("s2", "p2", "purchase", _ts(0), source="bing", medium="organic",
            value=597.0, transaction_id="T2"),
        _ev("s3", "r1", "refund", _ts(5), value=297.0, transaction_id="T1"),
    ])
    itens = _df_itens(spark, [
        _item("p1", "PLANO-BASICO", "plano"),
        _item("p2", "PLANO-PRO", "plano"),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compras = gra.compras_enriquecidas(eventos, itens, atributos)
    reembolsos = gra.reembolsos_atribuidos(eventos, compras)
    receita = gra.construir_receita_diaria(compras, reembolsos).collect()
    # exatamente duas linhas (uma por compra); nenhuma com pedidos=0
    assert len(receita) == 2
    for l in receita:
        assert l.pedidos >= 1
        assert l.receita_bruta >= l.receita_reembolsada


def test_ticket_medio_via_try_divide(spark):
    # Duas compras do mesmo (data, plano, canal): ticket = bruta/pedidos.
    eventos = _df_eventos(spark, [
        _ev("s1", "p1", "purchase", _ts(0), value=300.0, transaction_id="T1"),
        _ev("s2", "p2", "purchase", _ts(1), value=500.0, transaction_id="T2"),
    ])
    itens = _df_itens(spark, [
        _item("p1", "PLANO-BASICO", "plano"),
        _item("p2", "PLANO-BASICO", "plano"),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compras = gra.compras_enriquecidas(eventos, itens, atributos)
    reembolsos = gra.reembolsos_atribuidos(eventos, compras)
    receita = gra.construir_receita_diaria(compras, reembolsos).collect()
    assert len(receita) == 1
    assert receita[0].receita_bruta == 800.0
    assert receita[0].pedidos == 2
    assert receita[0].ticket_medio == 400.0


# --------------------------------------------------------------------------- #
# taxa_reembolso
# --------------------------------------------------------------------------- #

def test_taxa_reembolso_por_plano_canal(spark):
    # 3 compras PLANO-PRO / google/cpc; 1 reembolsada -> taxa = 1/3.
    eventos = _df_eventos(spark, [
        _ev("s1", "p1", "purchase", _ts(0), value=597.0, transaction_id="T1"),
        _ev("s2", "p2", "purchase", _ts(0), value=597.0, transaction_id="T2"),
        _ev("s3", "p3", "purchase", _ts(0), value=597.0, transaction_id="T3"),
        _ev("s4", "r1", "refund", _ts(5), value=597.0, transaction_id="T2"),
    ])
    itens = _df_itens(spark, [
        _item("p1", "PLANO-PRO", "plano"),
        _item("p2", "PLANO-PRO", "plano"),
        _item("p3", "PLANO-PRO", "plano"),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compras = gra.compras_enriquecidas(eventos, itens, atributos)
    reembolsos = gra.reembolsos_atribuidos(eventos, compras)
    taxa = gra.construir_taxa_reembolso(compras, reembolsos).collect()
    assert len(taxa) == 1
    linha = taxa[0]
    assert linha.pedidos == 3
    assert linha.pedidos_com_reembolso == 1
    assert linha.taxa_reembolso == 0.3333
    assert linha.pedidos_com_reembolso <= linha.pedidos


# --------------------------------------------------------------------------- #
# adesao_order_bump — ligação por event_id e item_id='BUMP-MENTORIA'
# --------------------------------------------------------------------------- #

def test_adesao_order_bump_por_event_id(spark):
    # 3 compras; 2 com BUMP-MENTORIA no items[] (ligado por event_id).
    eventos = _df_eventos(spark, [
        _ev("s1", "p1", "purchase", _ts(0), value=744.0, transaction_id="T1"),
        _ev("s2", "p2", "purchase", _ts(0), value=597.0, transaction_id="T2"),
        _ev("s3", "p3", "purchase", _ts(0), value=444.0, transaction_id="T3"),
    ])
    itens = _df_itens(spark, [
        _item("p1", "PLANO-PRO", "plano", index=0),
        _item("p1", "BUMP-MENTORIA", "bump", index=1),
        _item("p2", "PLANO-PRO", "plano", index=0),
        _item("p3", "PLANO-BASICO", "plano", index=0),
        _item("p3", "BUMP-MENTORIA", "bump", index=1),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compras = gra.compras_enriquecidas(eventos, itens, atributos)
    adesao = gra.construir_adesao_order_bump(compras, itens).collect()
    assert len(adesao) == 1
    linha = adesao[0]
    assert linha.compras == 3
    assert linha.compras_com_bump == 2
    assert linha.taxa_adesao == 0.6667
    assert linha.compras_com_bump <= linha.compras


def test_order_bump_nao_liga_por_transaction_id(spark):
    # O bump pertence a OUTRO event_id (p2), mas tem o mesmo transaction_id de p1.
    # A ligação é por event_id: só p2 conta como compra com bump, não p1.
    eventos = _df_eventos(spark, [
        _ev("s1", "p1", "purchase", _ts(0), value=597.0, transaction_id="T1"),
        _ev("s2", "p2", "purchase", _ts(0), value=744.0, transaction_id="T1"),
    ])
    itens = _df_itens(spark, [
        _item("p1", "PLANO-PRO", "plano"),
        _item("p2", "PLANO-PRO", "plano", index=0),
        _item("p2", "BUMP-MENTORIA", "bump", index=1),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    compras = gra.compras_enriquecidas(eventos, itens, atributos)
    adesao = gra.construir_adesao_order_bump(compras, itens).collect()[0]
    # 2 compras distintas por event_id; só p2 tem bump
    assert adesao.compras == 2
    assert adesao.compras_com_bump == 1


# --------------------------------------------------------------------------- #
# lead_para_compra
# --------------------------------------------------------------------------- #

def test_lead_para_compra_pessoa_estritamente_depois_e_dias(spark):
    # Pessoa com user_id_resolvido unindo duas sessões: lead no dia 1, compra no
    # dia 3 -> 2 dias. A compra deve ser ESTRITAMENTE depois do lead.
    eventos = _df_eventos(spark, [
        _ev("s1", "l1", "generate_lead", _ts(0, dia=1), user_pseudo_id="pseudo1",
            user_id_resolvido="user1", source="google", medium="cpc"),
        _ev("s2", "c1", "purchase", _ts(0, dia=3), user_pseudo_id="pseudo1",
            user_id_resolvido="user1", value=597.0, transaction_id="T1"),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    lead = gra.construir_lead_para_compra(eventos, atributos).collect()
    por_escopo = {(l.escopo, l.canal): l for l in lead}
    total = por_escopo[(gra.ESCOPO_TOTAL, gra.TODOS)]
    assert total.leads == 1
    assert total.leads_que_compraram == 1
    assert total.taxa_conversao == 1.0
    # 2 dias exatos entre dia 1 10:00 e dia 3 10:00
    assert total.mediana_dias == 2.0
    # canal do lead no escopo por_canal
    por_canal = por_escopo[(gra.ESCOPO_POR_CANAL, "google / cpc")]
    assert por_canal.leads == 1
    assert por_canal.leads_que_compraram == 1


def test_lead_para_compra_compra_antes_do_lead_nao_conta(spark):
    # Compra ANTES do lead: não conta como conversão (estritamente depois).
    eventos = _df_eventos(spark, [
        _ev("s1", "c1", "purchase", _ts(0, dia=1), user_pseudo_id="p1",
            value=597.0, transaction_id="T1"),
        _ev("s2", "l1", "generate_lead", _ts(0, dia=2), user_pseudo_id="p1"),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    lead = gra.construir_lead_para_compra(eventos, atributos).collect()
    total = [l for l in lead if l.escopo == gra.ESCOPO_TOTAL][0]
    assert total.leads == 1
    assert total.leads_que_compraram == 0
    assert total.taxa_conversao == 0.0
    assert total.mediana_dias is None  # ninguém comprou depois do lead


def test_lead_para_compra_mediana_fracionaria(spark):
    # Duas pessoas: uma converte em 1 dia, outra em 2 dias -> mediana via
    # percentile_approx (0.5). Com 2 valores {1,2}, percentile_approx 0.5 = 1.0.
    eventos = _df_eventos(spark, [
        _ev("s1", "l1", "generate_lead", _ts(0, dia=1), user_pseudo_id="pa"),
        _ev("s2", "c1", "purchase", _ts(0, dia=2), user_pseudo_id="pa",
            value=597.0, transaction_id="T1"),
        _ev("s3", "l2", "generate_lead", _ts(0, dia=1), user_pseudo_id="pb"),
        _ev("s4", "c2", "purchase", _ts(0, dia=3), user_pseudo_id="pb",
            value=597.0, transaction_id="T2"),
    ])
    atributos = gold_comum.atributos_sessao(eventos)
    lead = gra.construir_lead_para_compra(eventos, atributos).collect()
    total = [l for l in lead if l.escopo == gra.ESCOPO_TOTAL][0]
    assert total.leads == 2
    assert total.leads_que_compraram == 2
    # mediana de {1.0, 2.0} via percentile_approx(0.5) -> 1.0
    assert total.mediana_dias == 1.0
