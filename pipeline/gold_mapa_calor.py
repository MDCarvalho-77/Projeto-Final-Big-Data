"""Job Gold do mapa de calor: tabelas ``mapa_calor_cliques`` e
``mapa_calor_rolagem`` (R11, nomes de coluna EXATOS).

    docker compose run --rm app python pipeline/gold_mapa_calor.py

Lê os eventos válidos da Silver (``ler_eventos(spark, "silver")``) e constrói as
duas tabelas que o site (`site/index.html`, leitor de ``window.MAPA_CALOR``) e o
exportador R11 (`pipeline/exportar_mapa_calor.py`) consomem. Os nomes de coluna
são fixados pelo R11 e batem com o leitor do site — não renomear.

- ``mapa_calor_cliques`` (§7.1 do design): uma linha por (``pagina``,
  ``element_id``). A fonte é **qualquer** ``event_name`` com ``element_id`` e
  ``pagina`` não nulos (não só ``click`` — o 5.3 define a fonte pelos campos, e o
  ``element_id`` aparece em select_item, add_to_cart, video_start, file_download,
  generate_lead etc.). Colunas: ``pagina, element_id, cliques, sessoes,
  sessoes_na_pagina, taxa_clique, intensidade``.
- ``mapa_calor_rolagem`` (§7.2 do design): uma linha por marco ∈ {0,25,50,75,90}
  da landing (``pagina="/"``). Colunas: ``pagina, marco, sessoes_que_chegaram,
  pct_sessoes``.

Reaproveita ``pipeline/comum.py`` (R1/R2): acesso ao S3 só por Spark ``s3a://``;
Parquet; modo ANSI do Spark 4 (``F.try_divide`` onde o ``null`` é desejado).
Gravação Gold sem partição, ``overwrite`` (reconstruível, nunca acumula).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

import comum
from comum import criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo

# Nomes de coluna EXATOS exigidos pelo R11 (batem com o leitor de site/index.html).
COLUNAS_CLIQUES = [
    "pagina", "element_id", "cliques", "sessoes",
    "sessoes_na_pagina", "taxa_clique", "intensidade",
]
COLUNAS_ROLAGEM = ["pagina", "marco", "sessoes_que_chegaram", "pct_sessoes"]

# Marcos de rolagem da landing. O marco 0 é a base (todas as sessões com page_view
# na landing); 25/75/90 exigem scroll com percent_scrolled >= marco.
MARCOS_ROLAGEM = (0, 25, 50, 75, 90)

PAGINA_LANDING = "/"


# --------------------------------------------------------------------------- #
# §7.1 — mapa_calor_cliques
# --------------------------------------------------------------------------- #

def construir_mapa_calor_cliques(eventos: DataFrame) -> DataFrame:
    """R11/§7.1 — intensidade de cliques por (``pagina``, ``element_id``).

    Fonte: **qualquer** evento com ``element_id`` e ``pagina`` não nulos (não
    restringir ``event_name``). ``cliques`` conta eventos; ``sessoes`` conta
    sessões distintas que interagiram com o elemento; ``sessoes_na_pagina`` conta
    sessões distintas com ``page_view`` naquela ``pagina``. ``taxa_clique`` e
    ``intensidade`` usam ``F.try_divide`` (nulo quando o divisor é 0, modo ANSI) e
    são gravadas como ``double`` arredondado a 4 casas (não ``Decimal``)."""
    base = eventos.where(
        F.col("element_id").isNotNull() & F.col("pagina").isNotNull()
    )

    por_elemento = base.groupBy("pagina", "element_id").agg(
        F.count(F.lit(1)).alias("cliques"),
        F.countDistinct("session_id").alias("sessoes"),
    )

    # sessoes_na_pagina: sessões distintas com page_view por pagina (de todos os
    # eventos, não só os com element_id).
    sessoes_na_pagina = (
        eventos
        .where((F.col("event_name") == "page_view") & F.col("pagina").isNotNull())
        .groupBy("pagina")
        .agg(F.countDistinct("session_id").alias("sessoes_na_pagina"))
    )

    janela_pagina = Window.partitionBy("pagina")
    max_cliques = F.max("cliques").over(janela_pagina)

    df = (
        por_elemento
        .join(sessoes_na_pagina, "pagina", "left")
        .withColumn(
            "taxa_clique",
            F.round(F.try_divide(F.col("sessoes"), F.col("sessoes_na_pagina")), 4),
        )
        .withColumn(
            "intensidade",
            F.round(F.try_divide(F.col("cliques"), max_cliques), 4),
        )
    )
    return df.select(*COLUNAS_CLIQUES)


# --------------------------------------------------------------------------- #
# §7.2 — mapa_calor_rolagem
# --------------------------------------------------------------------------- #

def construir_mapa_calor_rolagem(eventos: DataFrame) -> DataFrame:
    """R11/§7.2 — profundidade de rolagem na landing (``pagina="/"``).

    Base (marco 0) = sessões distintas com ``page_view`` na landing. Para os
    marcos 25/50/75/90, uma sessão conta se teve ``scroll`` na landing com
    ``percent_scrolled >= marco``. ``pct_sessoes`` = ``sessoes_que_chegaram`` ÷
    base (``F.try_divide``, 4 casas). Sempre 5 linhas, ``pagina="/"``."""
    na_landing = eventos.where(F.col("pagina") == PAGINA_LANDING)

    sessoes_base = (
        na_landing
        .where(F.col("event_name") == "page_view")
        .select("session_id")
        .distinct()
    )
    base = sessoes_base.count()

    sessoes_scroll = (
        na_landing
        .where(F.col("event_name") == "scroll")
        .groupBy("session_id")
        .agg(F.max("percent_scrolled").alias("scroll_max"))
    )

    spark = eventos.sparkSession
    linhas = []
    for marco in MARCOS_ROLAGEM:
        if marco == 0:
            chegaram = base
        else:
            chegaram = sessoes_scroll.where(F.col("scroll_max") >= marco).count()
        linhas.append((PAGINA_LANDING, int(marco), int(chegaram)))

    df = spark.createDataFrame(linhas, ["pagina", "marco", "sessoes_que_chegaram"])
    df = df.withColumn(
        "pct_sessoes",
        F.round(F.try_divide(F.col("sessoes_que_chegaram"), F.lit(base)), 4),
    )
    return df.select(*COLUNAS_ROLAGEM)


# --------------------------------------------------------------------------- #
# Gravação e orquestração
# --------------------------------------------------------------------------- #

def _gravar(df: DataFrame, tabela: str) -> None:
    """Grava uma tabela Gold em ``gold/<tabela>/`` em Parquet, sem partição,
    ``overwrite`` (reconstruível, nunca acumula duplicatas)."""
    df.write.mode("overwrite").parquet(comum.caminho(f"gold/{tabela}"))


def main() -> int:
    spark = criar_sessao("gold_mapa_calor")
    try:
        exigir_camada(spark, glob_eventos("silver"), "python pipeline/silver.py")
        eventos = ler_eventos(spark, "silver").cache()

        cliques = construir_mapa_calor_cliques(eventos)
        rolagem = construir_mapa_calor_rolagem(eventos)

        _gravar(cliques, "mapa_calor_cliques")
        _gravar(rolagem, "mapa_calor_rolagem")

        titulo("Gold mapa de calor")
        n_cliques = cliques.count()
        print(f"[ok] mapa_calor_cliques: {n_cliques} linhas (pagina, element_id)")
        print(f"[ok] mapa_calor_rolagem: {rolagem.count()} linhas (marcos {list(MARCOS_ROLAGEM)})")

        eventos.unpersist()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
