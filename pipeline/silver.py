"""Job Silver: transforma a Bronze em dado limpo (R5–R10).

    docker compose run --rm app python pipeline/silver.py

O que este job faz, nesta ordem fixa e determinística (F1→F5):

- **F1 dedup (R5):** cada ``event_id`` aparece uma vez só. Cópias de reenvio de
  lote são removidas por ``row_number()`` com desempate estável. Linhas sem
  ``event_id`` são separadas antes da janela e mantidas isoladas (não viram
  duplicata umas das outras).
- **F2 tipagem (R6):** as chaves de ``params`` viram colunas tipadas, a partir do
  ``params_json`` cru (preserva número/booleano/nulo), incluindo ``element_id``.
  ``from_json``/``get_json_object`` fazem *parse* (não cast ANSI) e não abortam;
  todo cast posterior usa ``try_cast``/``try_to_timestamp``.
- **F3 validação (R7):** em dois passos (regras independentes + reembolso órfão
  contra as compras válidas), marca os motivos de rejeição. Os rejeitados vão
  para ``silver/eventos_rejeitados/`` com um motivo por linha.
- **F4 sessionização (R8):** corte estrito de >30 min por ``user_pseudo_id``, com
  window functions, **sem** usar ``session_hint``.
- **F5 identity stitching (R9):** todo evento de um ``user_pseudo_id`` que em
  algum momento teve ``user_id`` recebe esse ``user_id`` (inclusive anteriores ao
  login). O mapa é lido de válidos + rejeitados; a aplicação é só nos válidos.

Grava três conjuntos no prefixo ``silver/`` (prefixos disjuntos, ordem fixa
rejeitados → itens → válidos):

- ``silver/event_name=.../year=.../month=.../day=...`` — eventos válidos.
- ``silver/itens/year=.../month=.../day=...`` — ``items[]`` explodido (R10).
- ``silver/eventos_rejeitados/`` — eventos rejeitados com ``motivo_rejeicao``.

Reaproveita ``pipeline/comum.py`` (R1/R2): não reimplementa sessão, caminhos nem
IO. Acesso ao S3 só por Spark ``s3a://``; Parquet; modo ANSI do Spark 4.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

import comum
from comum import (
    criar_sessao,
    exigir_camada,
    glob_eventos,
    gravar_eventos,
    ler_eventos,
    titulo,
)

# --------------------------------------------------------------------------- #
# Constantes de módulo
# --------------------------------------------------------------------------- #

# Catálogo: os 22 event_name reais (o contrato conta 20 linhas, com video_*
# expandindo para três nomes). evento_fora_catalogo compara contra este conjunto;
# __HIVE_DEFAULT_PARTITION__ (event_name vazio) não pertence a ele, logo rejeita.
CATALOGO = (
    "page_view", "scroll", "click", "site_search",
    "video_start", "video_progress", "video_complete",
    "file_download", "view_item_list", "select_item", "view_item",
    "add_to_cart", "remove_from_cart", "view_cart", "begin_checkout",
    "add_shipping_info", "add_payment_info", "purchase", "refund",
    "generate_lead", "sign_up", "login",
)

# Os 6 motivos mínimos do enunciado (R7) + 2 acrescentados (justificados no
# RELATORIO.md). Rótulos são constantes, nunca interpolam valores, para agregação
# limpa ("rejeitados por motivo" sem mensagens variáveis).
MOTIVO_FORA_CATALOGO = "evento_fora_catalogo"
MOTIVO_TIMESTAMP = "timestamp_ausente_ou_futuro"
MOTIVO_TRANSACAO_SEM_ID = "transacao_sem_id"
MOTIVO_VALOR_INVALIDO = "valor_monetario_invalido"
MOTIVO_MARCO_FORA = "marco_fora_do_padrao"
MOTIVO_REEMBOLSO_ORFAO = "reembolso_sem_compra"
# acrescentados (fora das 6 mínimas):
MOTIVO_VALOR_NAO_NUMERICO = "valor_nao_numerico"
MOTIVO_MARCO_NAO_NUMERICO = "marco_nao_numerico"

MOTIVOS_MINIMOS = (
    MOTIVO_FORA_CATALOGO, MOTIVO_TIMESTAMP, MOTIVO_TRANSACAO_SEM_ID,
    MOTIVO_VALOR_INVALIDO, MOTIVO_MARCO_FORA, MOTIVO_REEMBOLSO_ORFAO,
)
MOTIVOS_ACRESCENTADOS = (MOTIVO_VALOR_NAO_NUMERICO, MOTIVO_MARCO_NAO_NUMERICO)
MOTIVOS = MOTIVOS_MINIMOS + MOTIVOS_ACRESCENTADOS

# Marcos válidos (R7).
MARCOS_SCROLL = (25, 50, 75, 90)
MARCOS_VIDEO = (0, 10, 25, 50, 75, 100)

# Pattern ISO-8601 UTC do event_ts (contrato: 2026-10-03T14:22:05.123Z).
# [.SSS] torna os milissegundos opcionais; [XXX] aceita o offset/'Z' opcional.
PATTERN_EVENT_TS = "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"

# Corte de sessão: estritamente > 30 min = 1800 s inicia nova sessão (R8).
LIMIAR_SESSAO_SEG = 1800

# Schema único (união) de todas as chaves tipadas da matriz de params do contrato.
# Cada chave com tipo único verificado chave-a-chave (value sempre double,
# currency sempre string, etc.). element_id é opcional e nunca rejeita.
SCHEMA_PARAMS_UNIAO = StructType([
    # engajamento / conteúdo
    StructField("page_title", StringType()),
    StructField("percent_scrolled", IntegerType()),
    StructField("link_url", StringType()),
    StructField("link_domain", StringType()),
    StructField("link_text", StringType()),
    StructField("link_id", StringType()),
    StructField("outbound", BooleanType()),
    StructField("search_term", StringType()),
    StructField("search_results", IntegerType()),
    StructField("search_area", StringType()),
    StructField("video_title", StringType()),
    StructField("video_provider", StringType()),
    StructField("video_duration", DoubleType()),
    StructField("video_current_time", DoubleType()),
    StructField("video_percent", IntegerType()),
    StructField("file_name", StringType()),
    StructField("file_extension", StringType()),
    # e-commerce
    StructField("item_list_id", StringType()),
    StructField("item_list_name", StringType()),
    StructField("currency", StringType()),
    StructField("value", DoubleType()),
    StructField("shipping_tier", StringType()),
    StructField("shipping", DoubleType()),
    StructField("payment_type", StringType()),
    StructField("transaction_id", StringType()),
    StructField("tax", DoubleType()),
    StructField("coupon", StringType()),
    StructField("reason", StringType()),
    # aquisição
    StructField("lead_source", StringType()),
    StructField("method", StringType()),
    # clique
    StructField("element_id", StringType()),
])

# Colunas tipadas promovidas de params (nome da coluna = nome da chave).
COLUNAS_PARAMS = [c.name for c in SCHEMA_PARAMS_UNIAO.fields]

# Nomes de colunas da Bronze reutilizados.
COL_PARAMS_JSON = "params_json"
COL_ARQUIVO_ORIGEM = "_arquivo_origem"
COL_INGERIDO_EM = "_ingerido_em"

# Colunas da tabela de rejeitados (rastreabilidade — R7).
COLUNAS_REJEITADOS = [
    "event_id", "event_name", "motivo_rejeicao",
    COL_ARQUIVO_ORIGEM, COL_INGERIDO_EM,
    "event_ts", "event_ts_ts", "user_pseudo_id", COL_PARAMS_JSON,
]


# --------------------------------------------------------------------------- #
# F2 — Tipagem de params (R6)  [vem antes da validação, que lê campos tipados]
# --------------------------------------------------------------------------- #

def _tipar_event_ts(df: DataFrame) -> DataFrame:
    """Adiciona ``event_ts_ts`` (timestamp) via ``try_to_timestamp`` com pattern
    explícito (§4.1 do design). Em modo ANSI, devolve ``null`` (não aborta) quando
    o texto não casa o ISO-8601 do contrato — comportamento de classificação
    desejado para R7 (timestamp ausente/mal-formado)."""
    return df.withColumn(
        "event_ts_ts",
        F.try_to_timestamp(F.col("event_ts"), F.lit(PATTERN_EVENT_TS)),
    )


def tipar_params(df: DataFrame) -> DataFrame:
    """R6 — extrai as chaves de ``params`` para colunas tipadas, a partir do
    ``params_json`` cru (preserva número/booleano/nulo; o map perderia o tipo).

    ``from_json``/``get_json_object`` fazem *parse* de JSON (não cast ANSI) e
    **não** abortam o job mesmo com ANSI ligado: campo mal-formado/incompatível
    vira ``null`` (classificação desejada, distinguindo ausência de valor
    inválido). Todo cast/conversão posterior usa ``try_cast``/``try_to_timestamp``.

    Materializa também as **flags de presença** (``tem_value``,
    ``tem_percent_scrolled``, ``tem_video_percent``) por
    ``get_json_object('$.<chave>') IS NOT NULL``: distinguem chave ausente
    (ausência legítima) de chave presente com valor inválido. Note que
    ``"value": null`` (JSON null) conta como ausente (``tem_value`` falso), o que
    é o tratamento desejado (não há quantia a validar).

    Preserva structs, items, ``params_json``/``params`` crus e proveniência."""
    df = _tipar_event_ts(df)
    tipados = F.from_json(F.col(COL_PARAMS_JSON), SCHEMA_PARAMS_UNIAO)
    df = df.withColumn("_params_tipados", tipados)
    for nome in COLUNAS_PARAMS:
        df = df.withColumn(nome, F.col("_params_tipados").getField(nome))
    df = df.drop("_params_tipados")

    def _tem(chave: str):
        return F.get_json_object(F.col(COL_PARAMS_JSON), f"$.{chave}").isNotNull()

    df = (
        df
        .withColumn("tem_value", _tem("value"))
        .withColumn("tem_percent_scrolled", _tem("percent_scrolled"))
        .withColumn("tem_video_percent", _tem("video_percent"))
    )
    return df


# --------------------------------------------------------------------------- #
# F1 — Deduplicação (R5)
# --------------------------------------------------------------------------- #

def deduplicar(bronze: DataFrame) -> DataFrame:
    """R5 — remove cópias por ``event_id``, de forma determinística.

    Linhas **sem** ``event_id`` são separadas **antes** da janela
    ``partitionBy("event_id")`` e mantidas integralmente: dois eventos sem
    ``event_id`` não são "o mesmo evento" (contrato: ``event_id`` é UUID único por
    evento emitido), então cada um sobrevive isolado — nunca tratado como
    duplicata de outro.

    Para as linhas com ``event_id``, mantém ``rn = 1`` ordenando por
    ``_arquivo_origem → _ingerido_em → event_ts → event_id`` (desempate estável e
    reproduzível; o reenvio de lote vem de outro arquivo). A proveniência da cópia
    escolhida é preservada (a própria linha carrega seu ``_arquivo_origem`` e
    ``_ingerido_em``)."""
    com_id = bronze.where(F.col("event_id").isNotNull())
    sem_id = bronze.where(F.col("event_id").isNull())

    janela = Window.partitionBy("event_id").orderBy(
        F.col(COL_ARQUIVO_ORIGEM).asc(),
        F.col(COL_INGERIDO_EM).asc(),
        F.col("event_ts").asc(),
        F.col("event_id").asc(),
    )
    com_id_rn1 = (
        com_id
        .withColumn("_rn", F.row_number().over(janela))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    return com_id_rn1.unionByName(sem_id)


# --------------------------------------------------------------------------- #
# 5.4 item 12 — normalização de `pagina`
# --------------------------------------------------------------------------- #

def normalizar_pagina(df: DataFrame) -> DataFrame:
    """5.4 item 12 — coluna ``pagina`` = caminho de ``page.location`` sem domínio,
    sem ``#`` e sem ``?``; a raiz vira ``/``. ``page.location`` nulo → ``pagina``
    nulo (não rejeita; não há regra R7 para página).

    Implementação determinística: extrai o caminho com ``parse_url(..., 'PATH')``
    (já remove esquema+host, ``#`` e ``?``) e mapeia caminho vazio para ``/``."""
    location = F.col("page.location")
    path = F.parse_url(location, F.lit("PATH"))
    pagina = (
        F.when(location.isNull(), F.lit(None).cast(StringType()))
        .when((path.isNull()) | (path == ""), F.lit("/"))
        .otherwise(path)
    )
    return df.withColumn("pagina", pagina)


# --------------------------------------------------------------------------- #
# F3 — Validação (R7)
# --------------------------------------------------------------------------- #

def classificar(df: DataFrame) -> DataFrame:
    """R7 passo 1 — marca os motivos de rejeição independentes (5 das 6 mínimas +
    a acrescentada ``valor_nao_numerico`` e ``marco_nao_numerico``).

    Cada motivo é uma coluna booleana com a condição **exata** do enunciado.
    ``reembolso_sem_compra`` é o passo 2 (``marcar_refund_orfao``), por depender do
    conjunto de ``purchase`` válidos. Condições null-safe: nenhum cast ANSI aborta;
    o ``null`` de conversão é sinal de defeito classificado, nunca mascarado."""
    e_transacao = F.col("event_name").isin("purchase", "refund")
    e_video = F.col("event_name").isin("video_start", "video_progress", "video_complete")
    e_scroll = F.col("event_name") == "scroll"

    # null-safe: event_name ausente (partição __HIVE_DEFAULT_PARTITION__ relida
    # como null) também é fora do catálogo — determinístico, sem depender de
    # null NOT IN (...) que avaliaria para desconhecido.
    fora_catalogo = (
        F.col("event_name").isNull()
        | ~F.col("event_name").isin(list(CATALOGO))
    )

    timestamp_ruim = F.col("event_ts_ts").isNull() | (
        F.col("event_ts_ts") > F.col(COL_INGERIDO_EM)
    )

    transacao_sem_id = e_transacao & (
        F.col("transaction_id").isNull() | (F.trim(F.col("transaction_id")) == "")
    )

    # valor_monetario_invalido: value < 0, OU (tem value e currency != BRL).
    # A cláusula de currency só dispara quando tem_value é verdadeiro (texto do
    # enunciado: "currency diferente de BRL em evento que tem value"). currency
    # ausente (null) não rejeita (ausência legítima de chave — decisão declarada).
    valor_invalido = (F.col("value") < 0) | (
        F.col("tem_value")
        & F.col("currency").isNotNull()
        & (F.col("currency") != "BRL")
    )

    # marco_fora_do_padrao null-safe: só marca quando o marco está PRESENTE e
    # tipado (IS NOT NULL) e fora do conjunto. Marco ausente/null não rejeita.
    marco_fora = (
        e_scroll
        & F.col("percent_scrolled").isNotNull()
        & ~F.col("percent_scrolled").isin(list(MARCOS_SCROLL))
    ) | (
        e_video
        & F.col("video_percent").isNotNull()
        & ~F.col("video_percent").isin(list(MARCOS_VIDEO))
    )

    # acrescentado: value presente (tem_value) mas não numérico (tipado null).
    valor_nao_numerico = F.col("tem_value") & F.col("value").isNull()

    # acrescentado: marco presente mas não numérico (tipado null).
    marco_nao_numerico = (
        e_scroll & F.col("tem_percent_scrolled") & F.col("percent_scrolled").isNull()
    ) | (
        e_video & F.col("tem_video_percent") & F.col("video_percent").isNull()
    )

    return (
        df
        .withColumn("_viol_fora_catalogo", fora_catalogo)
        .withColumn("_viol_timestamp", timestamp_ruim)
        .withColumn("_viol_transacao_sem_id", transacao_sem_id)
        .withColumn("_viol_valor_invalido", valor_invalido)
        .withColumn("_viol_marco_fora", marco_fora)
        .withColumn("_viol_valor_nao_numerico", valor_nao_numerico)
        .withColumn("_viol_marco_nao_numerico", marco_nao_numerico)
    )


# Colunas de violação do passo 1 (ordem fixa → ordem estável dos motivos).
_VIOL_PASSO1 = [
    ("_viol_fora_catalogo", MOTIVO_FORA_CATALOGO),
    ("_viol_timestamp", MOTIVO_TIMESTAMP),
    ("_viol_transacao_sem_id", MOTIVO_TRANSACAO_SEM_ID),
    ("_viol_valor_invalido", MOTIVO_VALOR_INVALIDO),
    ("_viol_marco_fora", MOTIVO_MARCO_FORA),
    ("_viol_valor_nao_numerico", MOTIVO_VALOR_NAO_NUMERICO),
    ("_viol_marco_nao_numerico", MOTIVO_MARCO_NAO_NUMERICO),
]


def _rejeitado_no_passo1(df: DataFrame):
    """Coluna booleana: violou alguma regra do passo 1."""
    cond = F.lit(False)
    for col, _ in _VIOL_PASSO1:
        cond = cond | F.col(col)
    return cond


def marcar_refund_orfao(df: DataFrame) -> DataFrame:
    """R7 passo 2 — ``reembolso_sem_compra``.

    Conjunto de ``transaction_id`` dos ``purchase`` **não rejeitados no passo 1**.
    Um ``refund`` que passou no passo 1 e cujo ``transaction_id`` não existe nesse
    conjunto recebe ``_viol_refund_orfao`` (join por ``transaction_id``, 5.4 item
    2: o reembolso é atribuído à compra pelo ``transaction_id``)."""
    rejeitado_p1 = _rejeitado_no_passo1(df)

    purchases_validos = (
        df
        .where((F.col("event_name") == "purchase") & ~rejeitado_p1)
        .select(F.col("transaction_id").alias("_tid_compra"))
        .where(F.col("_tid_compra").isNotNull())
        .distinct()
    )

    df = df.join(
        purchases_validos,
        F.col("transaction_id") == F.col("_tid_compra"),
        "left",
    )

    refund_orfao = (
        (F.col("event_name") == "refund")
        & ~rejeitado_p1
        & F.col("_tid_compra").isNull()
    )
    return df.withColumn("_viol_refund_orfao", refund_orfao).drop("_tid_compra")


def separar_validos_e_rejeitados(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Monta o array de motivos violados por evento e separa:

    - **válidos:** sem nenhum motivo (array vazio);
    - **rejeitados:** uma linha por (evento × motivo), via ``explode`` (R7: "um
      motivo por linha"). Rótulos constantes, rastreabilidade preservada.
    """
    pares = list(_VIOL_PASSO1) + [("_viol_refund_orfao", MOTIVO_REEMBOLSO_ORFAO)]
    motivos_arr = F.array_compact(F.array(*[
        F.when(F.col(col), F.lit(rotulo)) for col, rotulo in pares
    ]))
    df = df.withColumn("_motivos", motivos_arr)

    validos = df.where(F.size(F.col("_motivos")) == 0).drop("_motivos")
    for col, _ in pares:
        validos = validos.drop(col)

    rejeitados = (
        df
        .where(F.size(F.col("_motivos")) > 0)
        .withColumn("motivo_rejeicao", F.explode(F.col("_motivos")))
        .select(*COLUNAS_REJEITADOS)
    )
    return validos, rejeitados


# --------------------------------------------------------------------------- #
# F4 — Sessionização (R8)
# --------------------------------------------------------------------------- #

def sessionizar(validos: DataFrame) -> DataFrame:
    """R8 — ``session_id`` por corte estrito de >30 min por ``user_pseudo_id``.

    Window functions determinísticas; **nunca** usa ``session_hint``. Ordenação
    ``(event_ts_ts, event_id)`` (desempate por UUID estável). Roda só sobre os
    válidos (os rejeitados por timestamp não têm tempo utilizável). Expõe também
    ``ordinal`` (nº da sessão no navegador), útil à Gold para achar o 1º evento."""
    w = Window.partitionBy("user_pseudo_id").orderBy(
        F.col("event_ts_ts").asc(), F.col("event_id").asc()
    )
    anterior = F.lag("event_ts_ts").over(w)
    novo = anterior.isNull() | (
        (F.unix_timestamp("event_ts_ts") - F.unix_timestamp(anterior)) > LIMIAR_SESSAO_SEG
    )
    w_acum = (
        Window.partitionBy("user_pseudo_id")
        .orderBy(F.col("event_ts_ts").asc(), F.col("event_id").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        validos
        .withColumn("_novo", novo.cast(IntegerType()))
        .withColumn("ordinal", F.sum("_novo").over(w_acum))
        .withColumn(
            "session_id",
            F.concat_ws("#", F.col("user_pseudo_id"), F.col("ordinal").cast(StringType())),
        )
        .drop("_novo")
    )


# --------------------------------------------------------------------------- #
# F5 — Identity stitching (R9)
# --------------------------------------------------------------------------- #

def costurar_identidade(validos: DataFrame, dedup_todos: DataFrame) -> DataFrame:
    """R9 — ``user_id_resolvido`` por ``user_pseudo_id``.

    O mapa ``user_pseudo_id → user_id`` é lido de **todos** os eventos pós-dedup
    (válidos + rejeitados, via ``dedup_todos``): o ``user_id`` cru é campo de topo
    do evento e não depende de ``event_ts``, então um ``login``/``sign_up`` que
    fixa o ``user_id`` costura os eventos anteriores mesmo que ele próprio tenha
    sido rejeitado.

    Precedência quando há mais de um ``user_id`` para o mesmo ``user_pseudo_id``:
    o **primeiro no tempo** (``first(user_id, ignorenulls=True)`` sobre
    ``orderBy(event_ts_ts asc nulls last, event_id)``), de modo que um evento com
    timestamp válido fixe a identidade e um rejeitado por ts (``event_ts_ts``
    nulo) só seja usado na ausência de qualquer outro.

    A associação usa **só** ``user_pseudo_id → user_id``: dois ``user_pseudo_id``
    distintos nunca são unidos. O join é 1:1 por ``user_pseudo_id`` (o mapa tem uma
    linha por ``user_pseudo_id``), logo não multiplica eventos válidos. O
    ``user_id`` original é preservado para rastreabilidade.

    A precedência é resolvida por **um único mecanismo**: a janela ordenada acima
    com ``first(..., ignorenulls=True)`` (não se usa ``groupBy.agg(first)``, que
    não respeita ordenação). A redução a uma linha por ``user_pseudo_id`` é só um
    ``dropDuplicates`` sobre o valor já resolvido pela janela — não refaz a
    precedência."""
    dedup_todos = _tipar_event_ts(dedup_todos) if "event_ts_ts" not in dedup_todos.columns else dedup_todos

    w = Window.partitionBy("user_pseudo_id").orderBy(
        F.col("event_ts_ts").asc_nulls_last(), F.col("event_id").asc_nulls_last()
    )
    mapa = (
        dedup_todos
        .where(F.col("user_id").isNotNull())
        .withColumn("_user_id_mapa", F.first("user_id", ignorenulls=True).over(w))
        .select("user_pseudo_id", "_user_id_mapa")
        .dropDuplicates(["user_pseudo_id"])
    )

    return (
        validos
        .join(mapa, "user_pseudo_id", "left")
        .withColumn(
            "user_id_resolvido",
            F.coalesce(F.col("_user_id_mapa"), F.col("user_id")),
        )
        .drop("_user_id_mapa")
    )


# --------------------------------------------------------------------------- #
# R10 — Tabela de itens (silver/itens/)
# --------------------------------------------------------------------------- #

# Schema de saída explícito de silver/itens/ (contrato Silver→Gold).
CAMPOS_ITEM = [
    ("item_id", StringType()),
    ("item_name", StringType()),
    ("item_category", StringType()),
    ("price", DoubleType()),
    ("quantity", "long"),
    ("index", "long"),
    ("item_list_id", StringType()),
    ("item_list_name", StringType()),
]


def explodir_itens(validos: DataFrame) -> DataFrame:
    """R10 — uma linha por item por evento (``items[]`` explodido), sobre os
    válidos.

    Usa ``F.explode`` (não ``explode_outer``): ``items`` ``[]``/nulo → 0 linhas;
    N itens → N linhas. Não inventa itens artificiais. Cada linha carrega o
    vínculo com o evento de origem (``event_id``, ``event_name``, ``event_ts_ts``,
    ``user_pseudo_id``, ``session_id``, ``user_id_resolvido``) e a proveniência.

    Partição ``year/month/day`` derivada de ``event_ts_ts`` em UTC (dia do evento,
    5.4 itens 1 e 10). Como R10 explode só válidos, ``event_ts_ts`` é não-nulo."""
    item = F.col("_item")
    df = validos.withColumn("_item", F.explode("items"))

    selecao = [item.getField(nome).alias(nome) for nome, _ in CAMPOS_ITEM]
    selecao += [
        F.col("event_id"),
        F.col("event_name"),
        F.col("event_ts_ts"),
        F.col("user_pseudo_id"),
        F.col("user_id_resolvido"),
        F.col("session_id"),
        F.col(COL_ARQUIVO_ORIGEM),
        F.col(COL_INGERIDO_EM),
        F.date_format("event_ts_ts", "yyyy").alias("year"),
        F.date_format("event_ts_ts", "MM").alias("month"),
        F.date_format("event_ts_ts", "dd").alias("day"),
    ]
    return df.select(*selecao)


# --------------------------------------------------------------------------- #
# Orquestração
# --------------------------------------------------------------------------- #

def construir_silver(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Orquestra as fases F1→F5 e devolve ``(validos, rejeitados, itens,
    deduplicado)``.

    Falha com mensagem clara se a Bronze não existir (R2)."""
    exigir_camada(spark, glob_eventos("bronze"), "python pipeline/bronze.py")
    bronze = ler_eventos(spark, "bronze")

    # F1 dedup
    deduplicado = deduplicar(bronze)
    # F2 tipagem (event_ts_ts + colunas de params + flags) + pagina
    tipado = normalizar_pagina(tipar_params(deduplicado))
    # F3 validação (2 passos)
    classificado = marcar_refund_orfao(classificar(tipado))
    validos, rejeitados = separar_validos_e_rejeitados(classificado)
    # F4 sessionização (só válidos)
    validos = sessionizar(validos)
    # F5 stitching (mapa de válidos + rejeitados pós-dedup; aplicado aos válidos)
    validos = costurar_identidade(validos, tipado)
    # R10 itens (sobre válidos já sessionizados e costurados)
    itens = explodir_itens(validos)

    return validos, rejeitados, itens, deduplicado


def _gravar(validos: DataFrame, rejeitados: DataFrame, itens: DataFrame) -> None:
    """Grava os três conjuntos na ordem fixa rejeitados → itens → válidos, em
    prefixos disjuntos, sem apagar o dataset principal (§1.3 do design).

    Nunca grava na raiz ``silver/`` direto nem usa ``apagar_camada("silver")``."""
    (rejeitados.write.mode("overwrite")
        .parquet(comum.caminho("silver/eventos_rejeitados")))
    (itens.write.mode("overwrite").option("partitionOverwriteMode", "dynamic")
        .partitionBy("year", "month", "day").parquet(comum.caminho("silver/itens")))
    gravar_eventos(validos, "silver", "overwrite")


def _contar_rejeitados_distintos(rejeitados: DataFrame) -> int:
    """Conta eventos rejeitados **distintos** tratando cada linha sem ``event_id``
    como seu próprio evento. Método estável e determinístico (sem
    ``monotonically_increasing_id``): eventos distintos (por ``event_id`` não-nulo)
    colapsados + linhas com ``event_id`` nulo contadas uma a uma.

    Como uma linha por (evento × motivo), colapsa-se por ``event_id`` para contar
    eventos (não linhas)."""
    com_id = (
        rejeitados.where(F.col("event_id").isNotNull())
        .select("event_id").distinct().count()
    )
    sem_id = rejeitados.where(F.col("event_id").isNull()).count()
    return com_id + sem_id


def main() -> int:
    spark = criar_sessao("silver")
    try:
        validos, rejeitados, itens, deduplicado = construir_silver(spark)

        # Materializar os DataFrames usados para contar E gravar, para o log de
        # reconciliação refletir exatamente o que foi gravado (§6 do design).
        deduplicado = deduplicado.cache()
        validos = validos.cache()
        rejeitados = rejeitados.cache()
        itens = itens.cache()

        bronze_total = ler_eventos(spark, "bronze").count()
        eventos_unicos = deduplicado.count()
        duplicatas_removidas = bronze_total - eventos_unicos
        n_validos = validos.count()
        rejeitados_distintos = _contar_rejeitados_distintos(rejeitados)
        n_itens = itens.count()

        titulo("Silver")
        print(f"[ok] Processados (Bronze): {bronze_total}")
        print(f"[ok] Duplicatas removidas (R5): {duplicatas_removidas}")
        print(f"[ok] Eventos únicos (pós-dedup): {eventos_unicos}")
        print(f"[ok] Válidos: {n_validos}")
        print(f"[ok] Rejeitados (eventos distintos): {rejeitados_distintos}")
        print(f"[ok] Itens explodidos (R10): {n_itens}")

        print("\nRejeitados por motivo (linhas; um motivo por linha):")
        (rejeitados.groupBy("motivo_rejeicao").count()
            .orderBy(F.desc("count")).show(20, truncate=False))

        # Reconciliação (§6): identidades verificadas.
        id1 = bronze_total == (n_validos + duplicatas_removidas + rejeitados_distintos)
        intersec = (
            validos.where(F.col("event_id").isNotNull()).select("event_id")
            .intersect(
                rejeitados.where(F.col("event_id").isNotNull()).select("event_id")
            ).count()
        )
        id2 = intersec == 0
        id3 = (n_validos + rejeitados_distintos) == eventos_unicos

        # Invariantes de silver/itens/ (§6.1).
        soma_itens = (
            validos.where(F.size(F.coalesce(F.col("items"), F.array())) > 0)
            .select(F.explode("items")).count()
        )
        inv_itens_contagem = n_itens == soma_itens
        itens_orfaos = (
            itens.select("event_id").distinct()
            .subtract(validos.select("event_id")).count()
        )
        inv_itens_subset = itens_orfaos == 0

        print("\nReconciliação:")
        print(f"[{'ok' if id1 else 'FALHA'}] processados == válidos + duplicatas + rejeitados_distintos")
        print(f"[{'ok' if id2 else 'FALHA'}] disjunção válidos ∩ rejeitados (event_id) == 0")
        print(f"[{'ok' if id3 else 'FALHA'}] cobertura válidos + rejeitados_distintos == eventos_únicos")
        print(f"[{'ok' if inv_itens_contagem else 'FALHA'}] itens == Σ size(items) sobre válidos")
        print(f"[{'ok' if inv_itens_subset else 'FALHA'}] todo event_id de itens existe nos válidos")

        if not all([id1, id2, id3, inv_itens_contagem, inv_itens_subset]):
            raise SystemExit("Reconciliação falhou: ver identidades acima.")

        _gravar(validos, rejeitados, itens)
        print("\n[ok] Gravados: silver/eventos_rejeitados, silver/itens, silver/event_name=...")

        deduplicado.unpersist()
        validos.unpersist()
        rejeitados.unpersist()
        itens.unpersist()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
