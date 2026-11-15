"""Peças compartilhadas pelos jobs Gold (funil, engajamento, receita, ML).

    from gold_comum import atributos_sessao, to_date_utc

Centraliza as decisões transversais do design §1.2 (canal, plano e dispositivo da
sessão) e §1.1 (data em UTC), para que os cinco jobs Gold não as reimplementem.

Reaproveita ``pipeline/comum.py`` para o acesso ao S3 e à SparkSession — este
módulo só opera sobre DataFrames já lidos da Silver e **não** fala com o S3
diretamente. Modo ANSI do Spark 4 respeitado (não há cast que possa abortar).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Rótulo da sessão sem plano escolhido (5.4-3, literal).
PLANO_NENHUM = "(nenhum)"


def to_date_utc(coluna):
    """A data (UTC) de um ``timestamp`` do evento (design §1.1).

    A SparkSession fixa ``spark.sql.session.timeZone=UTC``, então ``F.to_date``
    sobre ``event_ts_ts`` devolve o dia do evento em UTC — nunca as partições
    ``year/month/day`` da camada, que são o dia de **coleta** (R4) e podem diferir
    do dia do evento (5.4-10). ``coluna`` pode ser um ``Column`` ou um nome."""
    return F.to_date(F.col(coluna) if isinstance(coluna, str) else coluna)


def atributos_sessao(eventos: DataFrame) -> DataFrame:
    """Canal, plano e dispositivo de cada sessão (design §1.2).

    Uma linha por ``session_id`` com:

    - ``canal`` = ``"<source> / <medium>"`` do **primeiro evento** da sessão
      (ordem ``event_ts_ts, event_id``). Cada componente é coalescido para string
      vazia **antes** do ``concat_ws``, de modo que o separador ``" / "`` esteja
      sempre presente (``null/null`` → ``" / "``; ``null/"cpc"`` → ``" / cpc"``):
      ``concat_ws`` pula argumentos nulos, o que deslocaria linhas entre grupos e
      quebraria a chave de agrupamento dos funis/receita (apontamento preservado
      de build-requirements e design §1.2).
    - ``dispositivo`` = ``device.category`` do primeiro evento da sessão.
    - ``plano`` = ``item_id`` do **primeiro ``select_item``** da sessão; ``"(nenhum)"``
      se a sessão não teve ``select_item`` (5.4-3, literal). O ``item_id`` vem de
      ``items[0].item_id`` do evento quando a Silver não promove ``item_id`` a
      coluna de topo (caso real: cada ``select_item`` carrega um único item no
      array ``items``); se já houver coluna ``item_id``, ela é usada diretamente.

    O "primeiro evento" usa o mesmo critério determinístico da sessionização em
    ``silver.py``: ``Window.partitionBy("session_id").orderBy(event_ts_ts,
    event_id)`` com ``F.first(..., ignorenulls=True)``. Para o plano, a janela é
    restrita aos eventos ``select_item`` antes do ``first``.
    """
    w = Window.partitionBy("session_id").orderBy(
        F.col("event_ts_ts").asc(), F.col("event_id").asc()
    )

    canal_evento = F.concat_ws(
        " / ",
        F.coalesce(F.col("traffic.source"), F.lit("")),
        F.coalesce(F.col("traffic.medium"), F.lit("")),
    )

    base = (
        eventos
        .withColumn("_canal", F.first(canal_evento, ignorenulls=True).over(w))
        .withColumn(
            "_dispositivo",
            F.first(F.col("device.category"), ignorenulls=True).over(w),
        )
    )
    canal_dispositivo = (
        base.select("session_id", "_canal", "_dispositivo")
        .dropDuplicates(["session_id"])
        .withColumnRenamed("_canal", "canal")
        .withColumnRenamed("_dispositivo", "dispositivo")
    )

    # Plano: primeiro item_id de um select_item da sessão (janela só sobre eles).
    w_plano = Window.partitionBy("session_id").orderBy(
        F.col("event_ts_ts").asc(), F.col("event_id").asc()
    )
    # A Silver real não promove ``item_id`` a coluna de topo: o plano do
    # ``select_item`` vem de ``items[0].item_id`` (cada ``select_item`` carrega
    # exatamente um item no array ``items``). DataFrames de teste que já tragam
    # uma coluna ``item_id`` de topo continuam suportados (mesma semântica).
    if "item_id" in eventos.columns:
        item_id_evento = F.col("item_id")
    else:
        item_id_evento = F.col("items").getItem(0).getField("item_id")
    plano = (
        eventos
        .where(F.col("event_name") == "select_item")
        .withColumn("_plano", F.first(item_id_evento, ignorenulls=True).over(w_plano))
        .select("session_id", "_plano")
        .dropDuplicates(["session_id"])
        .withColumnRenamed("_plano", "plano")
    )

    return (
        canal_dispositivo
        .join(plano, "session_id", "left")
        .withColumn("plano", F.coalesce(F.col("plano"), F.lit(PLANO_NENHUM)))
    )
