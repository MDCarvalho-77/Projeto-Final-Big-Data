"""Job Gold — engajamento (design §4; FEAT-002).

Lê a Silver (eventos válidos) e grava quatro tabelas analíticas em ``gold/``:

- ``gold/sessoes``            — resumo de cada visita (base do funil e do rótulo ML).
- ``gold/retencao_video``     — retenção do vídeo por marco {10,25,50,75,100}.
- ``gold/buscas_faq``         — buscas no FAQ por termo (total e sem resultado).
- ``gold/saidas_downloads``   — cliques de saída por domínio e downloads por arquivo.

Reaproveita ``pipeline/comum.py`` (SparkSession, caminhos s3a://, verificação de
camada) e ``pipeline/gold_comum.py`` (``atributos_sessao`` — canal e dispositivo
da sessão, design §1.2). O acesso ao S3 é só por Spark; a gravação é Parquet,
``overwrite`` por tabela e **sem** partição (design §1, decisão de gravação da
Gold). Modo ANSI do Spark 4 respeitado: toda taxa com denominador que pode ser
zero usa ``F.try_divide``.
"""

from __future__ import annotations

import comum
from comum import criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo
from gold_comum import atributos_sessao

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Marcos de retenção de vídeo (design §4.2). O marco 0 (video_start) é o
# denominador, não vira linha da tabela.
MARCOS_VIDEO = [10, 25, 50, 75, 100]


def _gravar(df: DataFrame, tabela: str) -> None:
    """Grava uma tabela Gold em ``gold/<tabela>`` (Parquet, overwrite, sem partição)."""
    df.write.mode("overwrite").parquet(comum.caminho(f"gold/{tabela}"))


# --------------------------------------------------------------------------- #
# §4.1 — sessoes
# --------------------------------------------------------------------------- #

def construir_sessoes(eventos: DataFrame) -> DataFrame:
    """Uma linha por ``session_id`` (design §4.1).

    Colunas (exatas): ``session_id, user_pseudo_id, user_id_resolvido, canal,
    dispositivo, inicio, fim, duracao_seg, n_eventos, scroll_max,
    video_percent_max, fez_lead, comprou``.

    - ``inicio``/``fim`` = ``min``/``max(event_ts_ts)``; ``duracao_seg`` =
      ``unix(fim) − unix(inicio)`` (≥ 0 por construção).
    - ``comprou`` = a sessão contém ``purchase`` (rótulo 5.4-6);
      ``fez_lead`` = a sessão contém ``generate_lead``.
    - ``canal``/``dispositivo`` vêm de ``atributos_sessao`` (design §1.2).
    """
    agregado = (
        eventos.groupBy("session_id")
        .agg(
            F.first("user_pseudo_id", ignorenulls=True).alias("user_pseudo_id"),
            F.first("user_id_resolvido", ignorenulls=True).alias("user_id_resolvido"),
            F.min("event_ts_ts").alias("inicio"),
            F.max("event_ts_ts").alias("fim"),
            F.count(F.lit(1)).alias("n_eventos"),
            F.max("percent_scrolled").alias("scroll_max"),
            F.max("video_percent").alias("video_percent_max"),
            F.max(F.when(F.col("event_name") == "generate_lead", F.lit(True)))
            .alias("_fez_lead"),
            F.max(F.when(F.col("event_name") == "purchase", F.lit(True)))
            .alias("_comprou"),
        )
        .withColumn(
            "duracao_seg",
            (F.unix_timestamp("fim") - F.unix_timestamp("inicio")).cast("long"),
        )
        .withColumn("fez_lead", F.coalesce(F.col("_fez_lead"), F.lit(False)))
        .withColumn("comprou", F.coalesce(F.col("_comprou"), F.lit(False)))
        .drop("_fez_lead", "_comprou")
    )

    atributos = atributos_sessao(eventos).select("session_id", "canal", "dispositivo")

    return (
        agregado.join(atributos, "session_id", "left")
        .select(
            "session_id",
            "user_pseudo_id",
            "user_id_resolvido",
            "canal",
            "dispositivo",
            "inicio",
            "fim",
            "duracao_seg",
            "n_eventos",
            "scroll_max",
            "video_percent_max",
            "fez_lead",
            "comprou",
        )
    )


# --------------------------------------------------------------------------- #
# §4.2 — retencao_video
# --------------------------------------------------------------------------- #

def construir_retencao_video(eventos: DataFrame) -> DataFrame:
    """Retenção do vídeo por marco (design §4.2).

    Denominador = sessões **com ``video_start``** (o ``video_start`` tem
    ``video_percent = 0``). Uma sessão atingiu o marco ``m`` se o máximo de
    ``video_percent`` da sessão é ``>= m``. As linhas cobrem só os marcos
    {10,25,50,75,100}; o marco 0 é o denominador, não vira linha.

    Colunas (exatas): ``marco, sessoes_no_marco, pct_retencao``.
    """
    sessoes_com_start = (
        eventos.where(F.col("event_name") == "video_start")
        .select("session_id")
        .distinct()
    )
    denominador = sessoes_com_start.count()

    # Máximo de video_percent por sessão elegível (que teve video_start).
    max_por_sessao = (
        eventos.where(F.col("event_name").isin("video_start", "video_progress", "video_complete"))
        .join(sessoes_com_start, "session_id", "inner")
        .groupBy("session_id")
        .agg(F.max("video_percent").alias("video_percent_max"))
    )

    spark = eventos.sparkSession
    marcos = spark.createDataFrame([(m,) for m in MARCOS_VIDEO], ["marco"])

    atingiu = (
        marcos.crossJoin(max_por_sessao)
        .where(F.col("video_percent_max") >= F.col("marco"))
        .groupBy("marco")
        .agg(F.count(F.lit(1)).alias("sessoes_no_marco"))
    )

    # Garante uma linha por marco, mesmo com 0 sessões (left join nos marcos).
    return (
        marcos.join(atingiu, "marco", "left")
        .withColumn("sessoes_no_marco", F.coalesce(F.col("sessoes_no_marco"), F.lit(0)).cast("long"))
        .withColumn(
            "pct_retencao",
            F.round(F.try_divide(F.col("sessoes_no_marco"), F.lit(denominador)), 4),
        )
        .select("marco", "sessoes_no_marco", "pct_retencao")
        .orderBy("marco")
    )


# --------------------------------------------------------------------------- #
# §4.3 — buscas_faq
# --------------------------------------------------------------------------- #

def construir_buscas_faq(eventos: DataFrame) -> DataFrame:
    """Buscas no FAQ por termo (design §4.3).

    Fonte: ``event_name = 'site_search'``. ``buscas`` = contagem;
    ``buscas_sem_resultado`` = número de buscas com ``search_results = 0``. O
    termo nulo é mantido como linha própria (registrado).

    Colunas (exatas): ``search_term, buscas, buscas_sem_resultado``.
    """
    return (
        eventos.where(F.col("event_name") == "site_search")
        .groupBy("search_term")
        .agg(
            F.count(F.lit(1)).alias("buscas"),
            F.sum(F.when(F.col("search_results") == 0, 1).otherwise(0))
            .cast("long")
            .alias("buscas_sem_resultado"),
        )
        .select("search_term", "buscas", "buscas_sem_resultado")
    )


# --------------------------------------------------------------------------- #
# §4.4 — saidas_downloads (+ build-requirements apontamento 4)
# --------------------------------------------------------------------------- #

def construir_saidas_downloads(eventos: DataFrame) -> DataFrame:
    """Cliques de saída por domínio e downloads por arquivo (design §4.4).

    Duas visões unificadas numa tabela:

    - ``tipo = 'saida'``: ``click`` com ``outbound = true``, agrupado por
      ``link_domain``. **Tratamento explícito de ``link_domain`` nulo
      (apontamento 4 de build-requirements):** uma saída exige ``link_domain``
      não nulo; a linha de saída com ``link_domain`` nulo é **descartada** da
      visão de saída (não vira ``chave = null``).
    - ``tipo = 'download'``: ``file_download``, agrupado por ``file_name``. Os
      downloads **nunca** dependem de ``link_domain`` — nenhum download legítimo
      é descartado por conta do tratamento das saídas.

    Colunas (exatas): ``tipo, chave, ocorrencias, sessoes``.
    """
    saidas = (
        eventos.where(
            (F.col("event_name") == "click")
            & (F.col("outbound") == True)  # noqa: E712 — comparação de coluna Spark
            & F.col("link_domain").isNotNull()  # apontamento 4: descarta saída sem domínio
        )
        .groupBy(F.col("link_domain").alias("chave"))
        .agg(
            F.count(F.lit(1)).alias("ocorrencias"),
            F.countDistinct("session_id").alias("sessoes"),
        )
        .withColumn("tipo", F.lit("saida"))
    )

    downloads = (
        eventos.where(F.col("event_name") == "file_download")
        .groupBy(F.col("file_name").alias("chave"))
        .agg(
            F.count(F.lit(1)).alias("ocorrencias"),
            F.countDistinct("session_id").alias("sessoes"),
        )
        .withColumn("tipo", F.lit("download"))
    )

    colunas = ["tipo", "chave", "ocorrencias", "sessoes"]
    return saidas.select(*colunas).unionByName(downloads.select(*colunas))


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def main() -> int:
    spark = criar_sessao("gold_engajamento")
    try:
        exigir_camada(spark, glob_eventos("silver"), "python pipeline/silver.py")  # R2
        eventos = ler_eventos(spark, "silver")

        sessoes = construir_sessoes(eventos)
        retencao = construir_retencao_video(eventos)
        buscas = construir_buscas_faq(eventos)
        saidas_downloads = construir_saidas_downloads(eventos)

        _gravar(sessoes, "sessoes")
        _gravar(retencao, "retencao_video")
        _gravar(buscas, "buscas_faq")
        _gravar(saidas_downloads, "saidas_downloads")

        titulo("Gold engajamento")
        print(f"sessoes:          {sessoes.count()} linhas")
        print(f"retencao_video:   {retencao.count()} linhas (marcos {MARCOS_VIDEO})")
        print(f"buscas_faq:       {buscas.count()} termos")
        print(f"saidas_downloads: {saidas_downloads.count()} linhas")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
