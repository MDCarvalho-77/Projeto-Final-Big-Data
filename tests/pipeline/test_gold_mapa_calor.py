"""Testes do job Gold do mapa de calor (pipeline/gold_mapa_calor.py).

Seguem o padrão de tests/pipeline/test_silver.py: fixture `spark` (local[2]) e
funções puras chamadas com DataFrames em memória no formato dos eventos válidos
da Silver (só as colunas que as funções leem: event_name, session_id, pagina,
element_id, percent_scrolled).

Cobrem o núcleo do R11:
- asserção LITERAL dos nomes de coluna das duas tabelas (ordem e conteúdo);
- cliques só de eventos com element_id e pagina não nulos, de QUALQUER event_name
  (não restrito a 'click');
- taxa_clique nula quando sessoes_na_pagina = 0 (try_divide);
- intensidade em [0,1] e máximo por pagina == 1;
- rolagem com marcos {0,25,50,75,90}, pagina='/', marco 0 => pct_sessoes = 1.0,
  monotonia decrescente e 4 casas decimais.
"""

from __future__ import annotations

import pytest
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

import gold_mapa_calor


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

# Subconjunto das colunas dos eventos válidos da Silver que o job lê.
_SCHEMA_EVENTOS = StructType([
    StructField("event_name", StringType()),
    StructField("session_id", StringType()),
    StructField("pagina", StringType()),
    StructField("element_id", StringType()),
    StructField("percent_scrolled", IntegerType()),
])


def _eventos(spark, linhas):
    """linhas = lista de (event_name, session_id, pagina, element_id, percent_scrolled)."""
    return spark.createDataFrame(linhas, schema=_SCHEMA_EVENTOS)


# --------------------------------------------------------------------------- #
# Colunas exatas (R11)
# --------------------------------------------------------------------------- #

def test_cliques_colunas_exatas_r11(spark):
    eventos = _eventos(spark, [
        ("page_view", "s1", "/", None, None),
        ("click", "s1", "/", "btn", None),
    ])
    df = gold_mapa_calor.construir_mapa_calor_cliques(eventos)
    assert df.columns == [
        "pagina", "element_id", "cliques", "sessoes",
        "sessoes_na_pagina", "taxa_clique", "intensidade",
    ]


def test_rolagem_colunas_exatas_r11(spark):
    eventos = _eventos(spark, [
        ("page_view", "s1", "/", None, None),
    ])
    df = gold_mapa_calor.construir_mapa_calor_rolagem(eventos)
    assert df.columns == ["pagina", "marco", "sessoes_que_chegaram", "pct_sessoes"]


# --------------------------------------------------------------------------- #
# mapa_calor_cliques
# --------------------------------------------------------------------------- #

def test_cliques_so_eventos_com_element_id_e_pagina(spark):
    eventos = _eventos(spark, [
        ("page_view", "s1", "/", None, None),      # sem element_id -> fora
        ("click", "s1", None, "btn", None),         # sem pagina -> fora
        ("click", "s1", "/", "btn", None),          # entra
    ])
    df = gold_mapa_calor.construir_mapa_calor_cliques(eventos)
    linhas = {(l["pagina"], l["element_id"]): l for l in df.collect()}
    assert set(linhas.keys()) == {("/", "btn")}
    assert linhas[("/", "btn")]["cliques"] == 1


def test_cliques_fonte_nao_restrita_a_click(spark):
    # element_id vem de eventos que NÃO são 'click' (select_item, video_start...)
    eventos = _eventos(spark, [
        ("page_view", "s1", "/", None, None),
        ("select_item", "s1", "/", "plano-pro", None),
        ("video_start", "s1", "/", "btn-play", None),
        ("file_download", "s2", "/", "dl-ementa", None),
    ])
    df = gold_mapa_calor.construir_mapa_calor_cliques(eventos)
    elementos = {l["element_id"] for l in df.collect()}
    assert elementos == {"plano-pro", "btn-play", "dl-ementa"}


def test_cliques_contagem_e_sessoes_distintas(spark):
    eventos = _eventos(spark, [
        ("page_view", "s1", "/", None, None),
        ("page_view", "s2", "/", None, None),
        ("click", "s1", "/", "cta", None),
        ("click", "s1", "/", "cta", None),   # mesma sessão clica 2x
        ("click", "s2", "/", "cta", None),
    ])
    df = gold_mapa_calor.construir_mapa_calor_cliques(eventos)
    l = df.where(df.element_id == "cta").collect()[0]
    assert l["cliques"] == 3          # 3 eventos
    assert l["sessoes"] == 2          # 2 sessões distintas
    assert l["sessoes_na_pagina"] == 2


def test_cliques_taxa_nula_quando_sem_page_view_na_pagina(spark):
    # pagina '/outra' tem clique mas nenhum page_view -> sessoes_na_pagina = null/0
    eventos = _eventos(spark, [
        ("click", "s1", "/outra", "x", None),
    ])
    df = gold_mapa_calor.construir_mapa_calor_cliques(eventos)
    l = df.where(df.pagina == "/outra").collect()[0]
    # sem page_view: left join deixa sessoes_na_pagina nulo -> try_divide -> nulo
    assert l["taxa_clique"] is None


def test_cliques_intensidade_em_0_1_e_max_por_pagina_igual_1(spark):
    eventos = _eventos(spark, [
        ("page_view", "s1", "/", None, None),
        ("page_view", "s2", "/", None, None),
        ("page_view", "s3", "/", None, None),
        # a: 3 cliques (mais clicado), b: 1 clique
        ("click", "s1", "/", "a", None),
        ("click", "s2", "/", "a", None),
        ("click", "s3", "/", "a", None),
        ("click", "s1", "/", "b", None),
    ])
    df = gold_mapa_calor.construir_mapa_calor_cliques(eventos)
    intens = {l["element_id"]: l["intensidade"] for l in df.collect()}
    assert all(0.0 <= v <= 1.0 for v in intens.values())
    assert max(intens.values()) == 1.0        # elemento mais clicado da pagina
    assert intens["a"] == 1.0
    assert intens["b"] == pytest.approx(0.3333, abs=1e-4)   # 1/3 arredondado a 4 casas


# --------------------------------------------------------------------------- #
# mapa_calor_rolagem
# --------------------------------------------------------------------------- #

def test_rolagem_marcos_e_pagina(spark):
    eventos = _eventos(spark, [
        ("page_view", "s1", "/", None, None),
    ])
    df = gold_mapa_calor.construir_mapa_calor_rolagem(eventos)
    linhas = df.collect()
    assert {l["marco"] for l in linhas} == {0, 25, 50, 75, 90}
    assert all(l["pagina"] == "/" for l in linhas)


def test_rolagem_marco_zero_e_100_porcento(spark):
    eventos = _eventos(spark, [
        ("page_view", "s1", "/", None, None),
        ("page_view", "s2", "/", None, None),
        ("scroll", "s1", "/", None, 90),   # s1 rola até 90
        ("scroll", "s2", "/", None, 50),   # s2 rola até 50
    ])
    df = gold_mapa_calor.construir_mapa_calor_rolagem(eventos)
    porc = {l["marco"]: (l["sessoes_que_chegaram"], l["pct_sessoes"]) for l in df.collect()}
    assert porc[0] == (2, 1.0)                    # base inteira = 100%
    assert porc[25][0] == 2                        # ambos >= 25
    assert porc[50][0] == 2                        # ambos >= 50
    assert porc[75][0] == 1                        # só s1 >= 75
    assert porc[90][0] == 1                        # só s1 >= 90
    assert porc[90][1] == pytest.approx(0.5, abs=1e-4)


def test_rolagem_monotonia_decrescente(spark):
    eventos = _eventos(spark, [
        ("page_view", "s1", "/", None, None),
        ("page_view", "s2", "/", None, None),
        ("page_view", "s3", "/", None, None),
        ("scroll", "s1", "/", None, 90),
        ("scroll", "s2", "/", None, 50),
        ("scroll", "s3", "/", None, 25),
    ])
    df = gold_mapa_calor.construir_mapa_calor_rolagem(eventos)
    pares = sorted((l["marco"], l["sessoes_que_chegaram"]) for l in df.collect())
    chegaram = [c for _, c in pares]
    assert chegaram == sorted(chegaram, reverse=True)   # decrescente em marco


def test_rolagem_pct_arredondado_4_casas(spark):
    # 3 sessões na base; 1 chega a 90 -> 1/3 = 0.3333
    eventos = _eventos(spark, [
        ("page_view", "s1", "/", None, None),
        ("page_view", "s2", "/", None, None),
        ("page_view", "s3", "/", None, None),
        ("scroll", "s1", "/", None, 90),
    ])
    df = gold_mapa_calor.construir_mapa_calor_rolagem(eventos)
    l90 = df.where(df.marco == 90).collect()[0]
    assert l90["pct_sessoes"] == pytest.approx(0.3333, abs=1e-9)
