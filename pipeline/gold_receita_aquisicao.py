"""Job Gold de receita e aquisição: ``receita_diaria``, ``taxa_reembolso``,
``adesao_order_bump`` e ``lead_para_compra`` (5.3, 5.4-2/3/5/7/8/9/10).

    docker compose run --rm app python pipeline/gold_receita_aquisicao.py

Quatro tabelas, todas em ``gold/`` sem partição, gravadas por
``df.write.mode("overwrite").parquet(comum.caminho("gold/<tabela>"))``
(reconstruíveis, sem acumular duplicatas):

- ``receita_diaria`` — receita bruta, reembolsada e líquida por
  (``data_compra``, ``plano``, ``canal``). A receita bruta de um pedido é o
  próprio ``value`` do ``purchase`` (5.4-2: já sem frete, com desconto Pix e
  incluindo o order bump — **não** somar os itens). O reembolso é atribuído à
  compra pelo ``transaction_id`` (join ``refund → purchase`` primeiro) e **herda
  da compra** a data (UTC), o plano e o canal, sendo lançado na **data da
  compra**. A união é um ``LEFT JOIN`` com a compra como base (todo ``refund``
  válido tem ``purchase`` — a Silver já rejeita órfãos), o que garante por
  construção que nenhuma linha tem ``pedidos=0`` por causa de reembolso e que
  ``receita_bruta >= receita_reembolsada`` em toda linha.
- ``taxa_reembolso`` — por (``plano``, ``canal``): pedidos, pedidos com ao menos
  um reembolso e a taxa (``try_divide``).
- ``adesao_order_bump`` — visão global: compras, compras que incluíram
  ``BUMP-MENTORIA`` e a taxa de adesão.
- ``lead_para_compra`` — por pessoa (``coalesce(user_id_resolvido,
  user_pseudo_id)``): do primeiro ``generate_lead`` à primeira compra
  **estritamente depois**; mediana de dias via ``percentile_approx``; escopos
  ``"(total)"`` e ``"por_canal"`` unidos.

Ligações (design §5.0 e build-requirements): ``purchase → itens`` (plano, order
bump) **sempre por ``event_id``** (``silver/itens`` não tem ``transaction_id``);
``refund → purchase`` por ``transaction_id``. Canal/plano da compra por
``gold_comum.atributos_sessao`` (via ``session_id``). Datas em UTC
(``gold_comum.to_date_utc``). Toda taxa usa ``F.try_divide`` (modo ANSI do
Spark 4).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

import comum
from comum import criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo
from gold_comum import PLANO_NENHUM, atributos_sessao, to_date_utc

# --------------------------------------------------------------------------- #
# Constantes de módulo
# --------------------------------------------------------------------------- #

# item_category do item de plano em silver/itens (5.4-3 / seção 2 do enunciado).
CATEGORIA_PLANO = "plano"
# item_id do order bump (seção 2 do enunciado).
ITEM_BUMP = "BUMP-MENTORIA"
# Rótulo do "todos" (total) nos escopos sem segmentação.
TODOS = "(todos)"
# Escopos de lead_para_compra (5.4-9).
ESCOPO_TOTAL = "(total)"
ESCOPO_POR_CANAL = "por_canal"
# Segundos em um dia (dias fracionários de lead→compra).
SEGUNDOS_POR_DIA = 86400.0
# Casas decimais das taxas.
CASAS_TAXA = 4


# --------------------------------------------------------------------------- #
# 5.0 — Bases comuns (compras enriquecidas e reembolsos atribuídos)
# --------------------------------------------------------------------------- #

def _plano_da_compra(itens: DataFrame) -> DataFrame:
    """Uma linha por ``event_id`` de ``purchase`` com o ``plano`` (``item_id`` do
    item de categoria ``plano``), ligado por ``event_id`` (design §5.0).

    ``silver/itens`` não tem ``transaction_id``: a ligação ``purchase → itens`` é
    SEMPRE por ``event_id``. Se houver mais de um item de categoria plano no mesmo
    evento (não esperado), o desempate é determinístico pelo menor ``index``."""
    w = Window.partitionBy("event_id").orderBy(F.col("index").asc_nulls_last())
    return (
        itens
        .where(F.col("item_category") == F.lit(CATEGORIA_PLANO))
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .select(F.col("event_id"), F.col("item_id").alias("plano"))
    )


def _event_ids_com_bump(itens: DataFrame) -> DataFrame:
    """``event_id`` distintos de eventos cujo ``items[]`` inclui o order bump
    (``item_id = 'BUMP-MENTORIA'``), ligados por ``event_id`` (design §5.3)."""
    return (
        itens
        .where(F.col("item_id") == F.lit(ITEM_BUMP))
        .select("event_id")
        .distinct()
    )


def compras_enriquecidas(
    eventos: DataFrame, itens: DataFrame, atributos: DataFrame
) -> DataFrame:
    """Base canônica de pedidos: uma linha por ``purchase`` válido (design §5.0).

    Enriquece cada ``purchase`` com:

    - ``data_compra`` = ``to_date(event_ts_ts)`` em UTC (5.4-10);
    - ``plano`` = item de categoria plano ligado por ``event_id`` (``"(nenhum)"``
      se o purchase não tiver item de plano — não esperado, mas determinístico);
    - ``canal`` = canal da sessão do purchase (``atributos_sessao`` por
      ``session_id``, 5.4-3);
    - ``receita_bruta`` = o próprio ``value`` do evento (5.4-2: NÃO somar itens).

    Mantém ``event_id``, ``transaction_id`` e ``session_id`` para as ligações
    seguintes (order bump por ``event_id``; reembolso por ``transaction_id``)."""
    plano = _plano_da_compra(itens)
    return (
        eventos
        .where(F.col("event_name") == F.lit("purchase"))
        .select(
            F.col("event_id"),
            F.col("transaction_id"),
            F.col("session_id"),
            F.col("value").alias("receita_bruta"),
            to_date_utc("event_ts_ts").alias("data_compra"),
        )
        .join(plano, "event_id", "left")
        .withColumn("plano", F.coalesce(F.col("plano"), F.lit(PLANO_NENHUM)))
        .join(atributos.select("session_id", "canal"), "session_id", "left")
    )


def reembolsos_atribuidos(eventos: DataFrame, compras: DataFrame) -> DataFrame:
    """Reembolsos atribuídos à compra por ``transaction_id`` (design §5.0,
    build-requirements apontamento 2): join ``refund → purchase`` PRIMEIRO.

    Cada ``refund`` válido herda da compra referenciada a ``data_compra``, o
    ``plano`` e o ``canal`` — para ser classificado na mesma linha da venda. A
    Silver já rejeita ``refund`` órfão (``reembolso_sem_compra``), então o
    ``inner join`` por ``transaction_id`` não descarta reembolso legítimo. O valor
    reembolsado é o ``value`` do próprio evento ``refund``."""
    refunds = (
        eventos
        .where(F.col("event_name") == F.lit("refund"))
        .select(
            F.col("transaction_id"),
            F.col("value").alias("valor_reembolsado"),
        )
    )
    # A compra fornece data/plano/canal; join por transaction_id (refund→purchase).
    chaves_compra = compras.select(
        "transaction_id", "data_compra", "plano", "canal"
    )
    return refunds.join(chaves_compra, "transaction_id", "inner")


# --------------------------------------------------------------------------- #
# 5.1 — receita_diaria
# --------------------------------------------------------------------------- #

def construir_receita_diaria(compras: DataFrame, reembolsos: DataFrame) -> DataFrame:
    """Tabela ``receita_diaria`` (design §5.1 + build-requirements apontamento 2).

    Granularidade: (``data_compra``, ``plano``, ``canal``). Agrega os ``purchase``
    (bruta e pedidos) e os ``refund`` atribuídos (reembolsada, lançada na DATA DA
    COMPRA e herdando plano/canal). Une por ``LEFT JOIN`` com a COMPRA como base
    (não ``FULL OUTER``): como todo ``refund`` válido tem ``purchase``, nenhuma
    linha nasce só de reembolso — logo nenhuma linha tem ``pedidos=0`` nem
    líquida negativa artificial, e ``receita_bruta >= receita_reembolsada`` vale
    por construção em toda linha.

    Colunas (ordem/nomes exatos): ``data_compra``, ``plano``, ``canal``,
    ``receita_bruta``, ``receita_reembolsada``, ``receita_liquida``, ``pedidos``,
    ``ticket_medio``.
    """
    chaves = ["data_compra", "plano", "canal"]

    agg_compras = compras.groupBy(*chaves).agg(
        F.sum("receita_bruta").alias("receita_bruta"),
        F.count(F.lit(1)).alias("pedidos"),
    )
    agg_reembolsos = reembolsos.groupBy(*chaves).agg(
        F.sum("valor_reembolsado").alias("receita_reembolsada")
    )

    return (
        agg_compras
        .join(agg_reembolsos, chaves, "left")
        .withColumn(
            "receita_reembolsada",
            F.coalesce(F.col("receita_reembolsada"), F.lit(0.0)),
        )
        .withColumn(
            "receita_liquida",
            F.col("receita_bruta") - F.col("receita_reembolsada"),
        )
        .withColumn(
            "ticket_medio",
            F.try_divide(F.col("receita_bruta"), F.col("pedidos")),
        )
        .select(
            "data_compra", "plano", "canal",
            "receita_bruta", "receita_reembolsada", "receita_liquida",
            "pedidos", "ticket_medio",
        )
    )


# --------------------------------------------------------------------------- #
# 5.2 — taxa_reembolso
# --------------------------------------------------------------------------- #

def construir_taxa_reembolso(compras: DataFrame, reembolsos: DataFrame) -> DataFrame:
    """Tabela ``taxa_reembolso`` (design §5.2).

    Granularidade: (``plano``, ``canal``). Uma compra conta como "com reembolso"
    se o seu ``transaction_id`` aparece ao menos uma vez nos ``refund`` atribuídos
    (compras distintas por ``transaction_id`` com ≥ 1 refund). A taxa usa
    ``F.try_divide`` (4 casas).

    Colunas (ordem/nomes exatos): ``plano``, ``canal``, ``pedidos``,
    ``pedidos_com_reembolso``, ``taxa_reembolso``.
    """
    tids_reembolsados = (
        reembolsos.select("transaction_id").where(F.col("transaction_id").isNotNull())
        .distinct()
        .withColumn("_tem_reembolso", F.lit(1))
    )
    compras_flag = (
        compras
        .join(tids_reembolsados, "transaction_id", "left")
        .withColumn("_tem_reembolso", F.coalesce(F.col("_tem_reembolso"), F.lit(0)))
    )
    return (
        compras_flag
        .groupBy("plano", "canal")
        .agg(
            F.count(F.lit(1)).alias("pedidos"),
            F.sum("_tem_reembolso").alias("pedidos_com_reembolso"),
        )
        .withColumn(
            "taxa_reembolso",
            F.round(
                F.try_divide(F.col("pedidos_com_reembolso"), F.col("pedidos")),
                CASAS_TAXA,
            ),
        )
        .select("plano", "canal", "pedidos", "pedidos_com_reembolso", "taxa_reembolso")
    )


# --------------------------------------------------------------------------- #
# 5.3 — adesao_order_bump
# --------------------------------------------------------------------------- #

def construir_adesao_order_bump(compras: DataFrame, itens: DataFrame) -> DataFrame:
    """Tabela ``adesao_order_bump`` (design §5.3).

    Visão global (uma linha). ``compras`` = ``purchase`` distintos por
    ``event_id``; ``compras_com_bump`` = compras cujo ``event_id`` aparece em
    ``silver/itens`` com ``item_id = 'BUMP-MENTORIA'`` (ligação por ``event_id``,
    nunca por ``transaction_id``). ``taxa_adesao`` via ``F.try_divide`` (4 casas).

    Colunas (ordem/nomes exatos): ``compras``, ``compras_com_bump``,
    ``taxa_adesao``.
    """
    com_bump = _event_ids_com_bump(itens).withColumn("_tem_bump", F.lit(1))
    compras_flag = (
        compras.select("event_id").distinct()
        .join(com_bump, "event_id", "left")
        .withColumn("_tem_bump", F.coalesce(F.col("_tem_bump"), F.lit(0)))
    )
    return compras_flag.agg(
        F.count(F.lit(1)).alias("compras"),
        F.sum("_tem_bump").alias("compras_com_bump"),
    ).withColumn(
        "taxa_adesao",
        F.round(F.try_divide(F.col("compras_com_bump"), F.col("compras")), CASAS_TAXA),
    ).select("compras", "compras_com_bump", "taxa_adesao")


# --------------------------------------------------------------------------- #
# 5.4 — lead_para_compra
# --------------------------------------------------------------------------- #

def construir_lead_para_compra(eventos: DataFrame, atributos: DataFrame) -> DataFrame:
    """Tabela ``lead_para_compra`` (design §5.4).

    Unidade = a pessoa (``coalesce(user_id_resolvido, user_pseudo_id)``, 5.4-5).
    Para cada pessoa: o primeiro ``generate_lead`` (menor ``event_ts_ts``) e a
    primeira compra **estritamente depois** dele. ``dias = (unix(compra) −
    unix(lead)) / 86400`` (fracionário). ``mediana_dias =
    percentile_approx(dias, 0.5)`` (5.4-8), só sobre quem comprou. O canal do lead
    é o canal da sessão do primeiro ``generate_lead`` (5.4-9). Escopos
    ``"(total)"`` e ``"por_canal"`` unidos por ``unionByName``.

    Colunas (ordem/nomes exatos): ``escopo``, ``canal``, ``leads``,
    ``leads_que_compraram``, ``taxa_conversao``, ``mediana_dias``.
    """
    pessoa = F.coalesce(F.col("user_id_resolvido"), F.col("user_pseudo_id"))
    base = eventos.withColumn("_pessoa", pessoa)

    # Primeiro generate_lead por pessoa: guarda ts e canal (da sessão do lead).
    leads = (
        base
        .where(F.col("event_name") == F.lit("generate_lead"))
        .join(atributos.select("session_id", "canal"), "session_id", "left")
    )
    w_lead = Window.partitionBy("_pessoa").orderBy(
        F.col("event_ts_ts").asc(), F.col("event_id").asc()
    )
    primeiro_lead = (
        leads
        .withColumn("_rn", F.row_number().over(w_lead))
        .where(F.col("_rn") == 1)
        .select(
            F.col("_pessoa"),
            F.col("event_ts_ts").alias("ts_lead"),
            F.col("canal").alias("canal_lead"),
        )
    )

    # Primeira compra por pessoa (menor ts de purchase).
    w_compra = Window.partitionBy("_pessoa").orderBy(
        F.col("event_ts_ts").asc(), F.col("event_id").asc()
    )
    primeira_compra = (
        base
        .where(F.col("event_name") == F.lit("purchase"))
        .withColumn("_rn", F.row_number().over(w_compra))
        .where(F.col("_rn") == 1)
        .select(F.col("_pessoa"), F.col("event_ts_ts").alias("ts_compra"))
    )

    # Uma linha por pessoa que fez lead; compra só conta se estritamente depois.
    pessoas = (
        primeiro_lead
        .join(primeira_compra, "_pessoa", "left")
        .withColumn(
            "_comprou",
            F.col("ts_compra").isNotNull() & (F.col("ts_compra") > F.col("ts_lead")),
        )
        .withColumn(
            "_dias",
            F.when(
                F.col("_comprou"),
                (F.unix_timestamp("ts_compra") - F.unix_timestamp("ts_lead"))
                / F.lit(SEGUNDOS_POR_DIA),
            ),
        )
    )

    def _agregar(df: DataFrame, chaves: list[str]) -> DataFrame:
        somas = [
            F.count(F.lit(1)).alias("leads"),
            F.sum(F.col("_comprou").cast("int")).alias("leads_que_compraram"),
            F.percentile_approx(F.col("_dias"), 0.5).alias("mediana_dias"),
        ]
        agregado = df.groupBy(*chaves).agg(*somas) if chaves else df.agg(*somas)
        return agregado.withColumn(
            "taxa_conversao",
            F.round(
                F.try_divide(F.col("leads_que_compraram"), F.col("leads")),
                CASAS_TAXA,
            ),
        )

    colunas = ["escopo", "canal", "leads", "leads_que_compraram", "taxa_conversao", "mediana_dias"]

    total = (
        _agregar(pessoas, chaves=[])
        .withColumn("escopo", F.lit(ESCOPO_TOTAL))
        .withColumn("canal", F.lit(TODOS))
        .select(*colunas)
    )
    por_canal = (
        _agregar(pessoas.withColumnRenamed("canal_lead", "canal"), chaves=["canal"])
        .withColumn("escopo", F.lit(ESCOPO_POR_CANAL))
        .select(*colunas)
    )
    return total.unionByName(por_canal)


# --------------------------------------------------------------------------- #
# Orquestração
# --------------------------------------------------------------------------- #

def _gravar(df: DataFrame, tabela: str) -> None:
    """Grava uma tabela Gold por overwrite total, sem partição (design §1)."""
    df.write.mode("overwrite").parquet(comum.caminho(f"gold/{tabela}"))


def main() -> int:
    spark = criar_sessao("gold_receita_aquisicao")
    try:
        exigir_camada(spark, glob_eventos("silver"), "python pipeline/silver.py")
        eventos = ler_eventos(spark, "silver")
        itens = spark.read.parquet(comum.caminho("silver/itens"))
        atributos = atributos_sessao(eventos).cache()

        compras = compras_enriquecidas(eventos, itens, atributos).cache()
        reembolsos = reembolsos_atribuidos(eventos, compras).cache()

        receita_diaria = construir_receita_diaria(compras, reembolsos).cache()
        taxa_reembolso = construir_taxa_reembolso(compras, reembolsos).cache()
        adesao_order_bump = construir_adesao_order_bump(compras, itens).cache()
        lead_para_compra = construir_lead_para_compra(eventos, atributos).cache()

        titulo("Gold receita e aquisição")
        print(f"[ok] receita_diaria: {receita_diaria.count()} linhas")
        (receita_diaria.agg(
            F.round(F.sum("receita_bruta"), 2).alias("bruta"),
            F.round(F.sum("receita_reembolsada"), 2).alias("reembolsada"),
            F.round(F.sum("receita_liquida"), 2).alias("liquida"),
            F.sum("pedidos").alias("pedidos"),
        ).show(truncate=False))
        # Invariante por linha: receita_bruta >= receita_reembolsada.
        viol = receita_diaria.where(
            F.col("receita_bruta") < F.col("receita_reembolsada")
        ).count()
        print(f"[{'ok' if viol == 0 else 'FALHA'}] receita_bruta >= receita_reembolsada em toda linha ({viol} violações)")
        print(f"[{'ok' if viol == 0 else 'FALHA'}] nenhuma linha de receita só-reembolso (pedidos>=1 garantido pelo LEFT JOIN)")
        if viol != 0:
            raise SystemExit("Invariante de receita violada: ver linhas acima.")

        print(f"[ok] taxa_reembolso: {taxa_reembolso.count()} linhas")
        taxa_reembolso.orderBy("plano", "canal").show(truncate=False)

        print("[ok] adesao_order_bump:")
        adesao_order_bump.show(truncate=False)

        print(f"[ok] lead_para_compra: {lead_para_compra.count()} linhas")
        lead_para_compra.orderBy(F.col("escopo"), "canal").show(truncate=False)

        _gravar(receita_diaria, "receita_diaria")
        _gravar(taxa_reembolso, "taxa_reembolso")
        _gravar(adesao_order_bump, "adesao_order_bump")
        _gravar(lead_para_compra, "lead_para_compra")
        print("[ok] Gravados: gold/receita_diaria, gold/taxa_reembolso, "
              "gold/adesao_order_bump, gold/lead_para_compra")

        atributos.unpersist()
        compras.unpersist()
        reembolsos.unpersist()
        receita_diaria.unpersist()
        taxa_reembolso.unpersist()
        adesao_order_bump.unpersist()
        lead_para_compra.unpersist()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
