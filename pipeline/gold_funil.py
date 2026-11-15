"""Job Gold do funil: tabelas ``funil_compra`` e ``funil_lead`` (5.3, 5.4-3/4/12).

    docker compose run --rm app python pipeline/gold_funil.py

Duas tabelas, ambas em ``gold/`` sem partição, gravadas por
``df.write.mode("overwrite").parquet(comum.caminho("gold/<tabela>"))``
(reconstruíveis, sem acumular duplicatas):

- ``funil_compra`` — sessões que atingiram cada etapa da jornada de compra, com
  conversão sobre a etapa anterior e sobre o topo, no total e por plano × canal
  (``escopo`` distingue as visões numa só tabela). ``view_item`` não é etapa da
  sequência (5.3): vira a coluna ``taxa_view_item`` = sessões com ``view_item`` ÷
  sessões com ``select_item``, **no mesmo escopo da linha** (global no total; por
  par plano×canal no segmentado — apontamento 1 de build-requirements).
- ``funil_lead`` — por canal da landing: ``page_view`` → ``scroll ≥ 50%`` →
  ``generate_lead``, com uma linha-sentinela ``canal='(total)'`` (toda agregação
  por canal deve filtrar ``canal <> '(total)'``).

Lê a Silver por ``ler_eventos(spark, "silver")`` (R1/R2), reaproveita
``gold_comum.atributos_sessao`` (canal/plano/dispositivo da sessão, design §1.2) e
usa ``F.try_divide`` em toda taxa (modo ANSI do Spark 4).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import comum
from comum import criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo
from gold_comum import atributos_sessao

# --------------------------------------------------------------------------- #
# Constantes de módulo
# --------------------------------------------------------------------------- #

# As 7 etapas da jornada de compra, em ordem (5.4-4). view_item NÃO entra aqui
# (5.3): é a taxa_view_item à parte.
ETAPAS_COMPRA = (
    "view_item_list",
    "select_item",
    "add_to_cart",
    "view_cart",
    "begin_checkout",
    "add_payment_info",
    "purchase",
)

# Rótulos do "todos" no escopo total.
TODOS = "(todos)"
# Rótulo da linha-sentinela do funil_lead.
TOTAL_LEAD = "(total)"

# Marco mínimo de scroll na landing para o funil_lead (contrato: 25/50/75/90).
SCROLL_LEAD = 50


# --------------------------------------------------------------------------- #
# funil_compra
# --------------------------------------------------------------------------- #

def _presenca_por_sessao(eventos: DataFrame) -> DataFrame:
    """Uma linha por ``session_id`` com, para cada etapa e para ``view_item``, um
    booleano (``max``) indicando se o evento ocorreu **em qualquer momento** da
    sessão (5.4-4: a sessão conta na etapa sem exigir as etapas anteriores)."""
    agregacoes = [
        F.max((F.col("event_name") == etapa).cast("int")).alias(f"tem_{etapa}")
        for etapa in ETAPAS_COMPRA
    ]
    agregacoes.append(
        F.max((F.col("event_name") == "view_item").cast("int")).alias("tem_view_item")
    )
    return eventos.groupBy("session_id").agg(*agregacoes)


def _etapas_longas(
    recortes: DataFrame, cols_recorte: list[str]
) -> DataFrame:
    """Converte a visão larga (uma coluna ``tem_<etapa>`` por etapa) para o
    formato longo (uma linha por etapa), somando as sessões por recorte.

    ``recortes`` traz as colunas de chave (``cols_recorte``), as somas
    ``s_<etapa>`` e as auxiliares ``s_view_item``/``s_select_item`` da taxa."""
    linhas = []
    for ordem, etapa in enumerate(ETAPAS_COMPRA, start=1):
        sel = [F.col(c) for c in cols_recorte]
        sel += [
            F.lit(etapa).alias("etapa"),
            F.lit(ordem).alias("ordem_etapa"),
            F.col(f"s_{etapa}").alias("sessoes"),
            # taxa_view_item no MESMO escopo da linha (repetida em todas as etapas
            # do recorte): sessões com view_item ÷ sessões com select_item.
            F.try_divide(F.col("s_view_item"), F.col("s_select_item")).alias("taxa_view_item"),
        ]
        linhas.append(recortes.select(*sel))
    longo = linhas[0]
    for parte in linhas[1:]:
        longo = longo.unionByName(parte)
    return longo


def _somar_recorte(
    presenca: DataFrame, chaves: list[str]
) -> DataFrame:
    """Soma, por recorte (``chaves``; vazio = total global), as sessões em cada
    etapa, mais as auxiliares de ``taxa_view_item``."""
    somas = [
        F.sum(F.col(f"tem_{etapa}")).alias(f"s_{etapa}") for etapa in ETAPAS_COMPRA
    ]
    # s_select_item já é produzido no laço acima (select_item é etapa 2); aqui só
    # falta a soma de view_item, usada no numerador de taxa_view_item.
    somas.append(F.sum(F.col("tem_view_item")).alias("s_view_item"))
    if chaves:
        return presenca.groupBy(*chaves).agg(*somas)
    return presenca.agg(*somas)


def _conversoes(longo: DataFrame, cols_recorte: list[str]) -> DataFrame:
    """Acrescenta ``conv_etapa_anterior`` e ``conv_topo`` por recorte.

    Ambas com ``F.try_divide`` (denominador pode ser 0 num recorte). A conversão
    sobre o topo divide pela etapa 1 (``view_item_list``) do mesmo recorte; a
    conversão anterior divide pela etapa imediatamente anterior (``ordem_etapa -
    1``). Faz dois self-joins pela chave do recorte + ``ordem_etapa``."""
    topo = (
        longo.where(F.col("ordem_etapa") == 1)
        .select(*cols_recorte, F.col("sessoes").alias("_sessoes_topo"))
    )
    anterior = longo.select(
        *cols_recorte,
        (F.col("ordem_etapa") + 1).alias("ordem_etapa"),
        F.col("sessoes").alias("_sessoes_anterior"),
    )
    # Com recorte vazio (escopo total) o "topo" é um único valor: cross join.
    # Com recorte, a chave do topo é o próprio recorte.
    if cols_recorte:
        longo = longo.join(topo, cols_recorte, "left")
    else:
        longo = longo.crossJoin(topo)
    chaves_ant = cols_recorte + ["ordem_etapa"]
    return (
        longo
        .join(anterior, chaves_ant, "left")
        .withColumn(
            "conv_etapa_anterior",
            F.try_divide(F.col("sessoes"), F.col("_sessoes_anterior")),
        )
        .withColumn(
            "conv_topo",
            F.try_divide(F.col("sessoes"), F.col("_sessoes_topo")),
        )
        .drop("_sessoes_topo", "_sessoes_anterior")
    )


def construir_funil_compra(eventos: DataFrame, atributos: DataFrame) -> DataFrame:
    """Tabela ``funil_compra`` (design §3.1).

    Granularidade: (``escopo``, ``plano``, ``canal``, ``etapa``). O escopo
    ``total`` resume todas as sessões (``plano``/``canal`` = ``"(todos)"``); o
    ``segmentado`` agrupa por par plano×canal (de ``gold_comum.atributos_sessao``).

    Colunas (ordem/nomes exatos): ``escopo``, ``plano``, ``canal``, ``etapa``,
    ``ordem_etapa``, ``sessoes``, ``conv_etapa_anterior``, ``conv_topo``,
    ``taxa_view_item``.
    """
    presenca = _presenca_por_sessao(eventos)

    # ----- escopo total -----
    somas_total = _somar_recorte(presenca, chaves=[])
    longo_total = _etapas_longas(somas_total, cols_recorte=[])
    longo_total = _conversoes(longo_total, cols_recorte=[])
    tabela_total = (
        longo_total
        .withColumn("escopo", F.lit("total"))
        .withColumn("plano", F.lit(TODOS))
        .withColumn("canal", F.lit(TODOS))
    )

    # ----- escopo segmentado (por plano × canal da sessão) -----
    presenca_seg = presenca.join(
        atributos.select("session_id", "plano", "canal"), "session_id", "left"
    )
    somas_seg = _somar_recorte(presenca_seg, chaves=["plano", "canal"])
    longo_seg = _etapas_longas(somas_seg, cols_recorte=["plano", "canal"])
    longo_seg = _conversoes(longo_seg, cols_recorte=["plano", "canal"])
    tabela_seg = longo_seg.withColumn("escopo", F.lit("segmentado"))

    colunas = [
        "escopo", "plano", "canal", "etapa", "ordem_etapa", "sessoes",
        "conv_etapa_anterior", "conv_topo", "taxa_view_item",
    ]
    return tabela_total.select(*colunas).unionByName(tabela_seg.select(*colunas))


# --------------------------------------------------------------------------- #
# funil_lead
# --------------------------------------------------------------------------- #

def _flags_lead_por_sessao(eventos: DataFrame) -> DataFrame:
    """Uma linha por ``session_id`` com os três marcos do funil de lead e o canal:

    - ``tem_pv_landing`` — ``page_view`` com ``pagina = "/"`` (5.4-12: a landing);
    - ``tem_scroll50_landing`` — ``scroll`` na landing com ``percent_scrolled ≥ 50``;
    - ``tem_lead`` — ``generate_lead`` em qualquer momento da sessão.
    """
    na_landing = F.col("pagina") == F.lit("/")
    pv = (F.col("event_name") == "page_view") & na_landing
    scroll50 = (
        (F.col("event_name") == "scroll")
        & na_landing
        & (F.col("percent_scrolled") >= F.lit(SCROLL_LEAD))
    )
    lead = F.col("event_name") == "generate_lead"
    return eventos.groupBy("session_id").agg(
        F.max(pv.cast("int")).alias("tem_pv_landing"),
        F.max(scroll50.cast("int")).alias("tem_scroll50_landing"),
        F.max(lead.cast("int")).alias("tem_lead"),
    )


def _agregar_lead(flags: DataFrame, chaves: list[str]) -> DataFrame:
    """Soma as sessões em cada marco por recorte (``chaves`` vazio = total)."""
    somas = [
        F.sum("tem_pv_landing").alias("sessoes_page_view"),
        F.sum("tem_scroll50_landing").alias("sessoes_scroll_50"),
        F.sum("tem_lead").alias("sessoes_lead"),
    ]
    if chaves:
        return flags.groupBy(*chaves).agg(*somas)
    return flags.agg(*somas)


def construir_funil_lead(eventos: DataFrame, atributos: DataFrame) -> DataFrame:
    """Tabela ``funil_lead`` (design §3.2).

    Granularidade: uma linha por ``canal`` + a linha-sentinela ``canal='(total)'``
    (acréscimo opcional; toda agregação por canal deve filtrar
    ``canal <> '(total)'``).

    Colunas (ordem/nomes exatos): ``canal``, ``sessoes_page_view``,
    ``sessoes_scroll_50``, ``sessoes_lead``, ``conv_scroll``, ``conv_lead``.
    """
    flags = _flags_lead_por_sessao(eventos).join(
        atributos.select("session_id", "canal"), "session_id", "left"
    )

    por_canal = _agregar_lead(flags, chaves=["canal"])
    total = _agregar_lead(flags, chaves=[]).withColumn("canal", F.lit(TOTAL_LEAD))

    colunas_base = ["canal", "sessoes_page_view", "sessoes_scroll_50", "sessoes_lead"]
    unido = por_canal.select(*colunas_base).unionByName(total.select(*colunas_base))

    return (
        unido
        .withColumn(
            "conv_scroll",
            F.try_divide(F.col("sessoes_scroll_50"), F.col("sessoes_page_view")),
        )
        .withColumn(
            "conv_lead",
            F.try_divide(F.col("sessoes_lead"), F.col("sessoes_scroll_50")),
        )
    )


# --------------------------------------------------------------------------- #
# Orquestração
# --------------------------------------------------------------------------- #

def _gravar(df: DataFrame, tabela: str) -> None:
    """Grava uma tabela Gold por overwrite total, sem partição (design §1)."""
    df.write.mode("overwrite").parquet(comum.caminho(f"gold/{tabela}"))


def main() -> int:
    spark = criar_sessao("gold_funil")
    try:
        exigir_camada(spark, glob_eventos("silver"), "python pipeline/silver.py")
        eventos = ler_eventos(spark, "silver")
        atributos = atributos_sessao(eventos).cache()

        funil_compra = construir_funil_compra(eventos, atributos).cache()
        funil_lead = construir_funil_lead(eventos, atributos).cache()

        titulo("Gold funil")
        print(f"[ok] funil_compra: {funil_compra.count()} linhas")
        print("funil_compra (escopo total):")
        (funil_compra.where(F.col("escopo") == "total")
            .orderBy("ordem_etapa")
            .show(truncate=False))
        print(f"[ok] funil_lead: {funil_lead.count()} linhas")
        funil_lead.orderBy(F.col("canal") == TOTAL_LEAD, "canal").show(truncate=False)

        _gravar(funil_compra, "funil_compra")
        _gravar(funil_lead, "funil_lead")
        print("[ok] Gravados: gold/funil_compra, gold/funil_lead")

        atributos.unpersist()
        funil_compra.unpersist()
        funil_lead.unpersist()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
