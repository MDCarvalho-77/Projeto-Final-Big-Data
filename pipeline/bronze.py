"""Job Bronze: ingestão fiel dos eventos da landing para a camada bronze (R1, R2, R4).

    docker compose run --rm app python pipeline/bronze.py

O que este job faz, e só isto (a limpeza é da Silver):

- lê os eventos de ``landing/event_name=.../year=.../month=.../day=...`` com um
  SCHEMA EXPLÍCITO (R4), sem inferência, compatível com docs/CONTRATO_EVENTO.md:
  os campos simples, os objetos aninhados (page, traffic, device), a lista items
  e params preservado em duas colunas: ``params_json`` (o objeto JSON como texto,
  fiel aos tipos/aninhamento/nulos) e ``params`` (map<string,string> derivado dele,
  consultável). A tipagem por chave é trabalho da Silver (R6);
- acrescenta as colunas de PROVENIÊNCIA (R4): o arquivo de origem na landing e o
  instante da ingestão;
- grava em Parquet no bucket datalake-tracking, por s3a:// (R1), particionado por
  event_name/year/month/day (R4), em modo APPEND (append-only, R4).

Reaproveita pipeline/comum.py (R1/R2): não reimplementa sessão, caminhos nem IO.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from comum import criar_sessao, exigir_camada, glob_eventos, gravar_eventos, ler_eventos, titulo

# --------------------------------------------------------------------------- #
# Schema explícito da landing (R4), fiel ao docs/CONTRATO_EVENTO.md.
#
# Observações sobre tipos:
# - event_ts fica como string nesta camada: a Bronze é cópia fiel, e o evento
#   pode trazer timestamp nulo ou mal formado (defeitos que a Silver rejeita);
#   converter aqui, no modo ANSI do Spark 4, pararia o job. A tipagem para
#   timestamp é da Silver.
# - params: as chaves variam por evento (contrato, tabela "params por evento") e
#   os valores têm tipos JSON diferentes (texto, número, booleano, nulo e, em
#   eventos inesperados, objeto/array aninhado). Para ser FIEL (Bronze) sem virar
#   camada de limpeza, a Bronze guarda params de duas formas, derivadas de uma
#   única leitura:
#     * params_json (string): o OBJETO JSON de params como texto, preservando os
#       TIPOS (número, booleano, nulo), o aninhamento e as chaves inesperadas. O
#       Spark reserializa o objeto (o espaçamento pode normalizar), mas o conteúdo
#       e os tipos são fiéis: nada é perdido nem inventado.
#     * params (map<string,string>): derivado de params_json com from_json, para
#       consultar as chaves no explorador e servir de base à Silver. O mapa perde
#       só a ETIQUETA de tipo JSON (número/booleano viram texto; objeto/array
#       aninhado viram o texto JSON do valor), mas o texto fica intacto e o tipo
#       correto é recuperável do params_json. A tipagem por chave é a R6 (Silver).
#   Por que não VariantType: foi testado (lê/grava/relê no Spark e no DuckDB do
#   explorador) e funciona, mas é um tipo mais novo, muda mais a camada e o
#   catálogo, e a dupla params_json + map já é fiel com a menor alteração. A
#   decisão e os testes estão em entregas/01_bronze/evidencias/05_decisao_params.md.
# - as colunas de partição event_name/year/month/day NÃO entram no schema de
#   leitura: vêm do caminho (ler_eventos/basePath) e o Spark as acrescenta.
#   O event_name está dentro de cada evento (contrato) E no caminho da partição,
#   com o mesmo valor. Para não duplicar a coluna (nem depender do desempate do
#   Spark entre schema e partição), ele é OMITIDO do schema e assumido da
#   partição (ver docs/CONTRATO_EVENTO.md e o layout da landing).
# --------------------------------------------------------------------------- #

PAGE = StructType([
    StructField("location", StringType()),
    StructField("referrer", StringType()),
])

TRAFFIC = StructType([
    StructField("source", StringType()),
    StructField("medium", StringType()),
    StructField("campaign", StringType()),
])

DEVICE = StructType([
    StructField("category", StringType()),
    StructField("os", StringType()),
    StructField("browser", StringType()),
])

ITEM = StructType([
    StructField("item_id", StringType()),
    StructField("item_name", StringType()),
    StructField("item_category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", LongType()),
    StructField("index", LongType()),
    StructField("item_list_id", StringType()),
    StructField("item_list_name", StringType()),
])

# params é lido como STRING: assim o Spark entrega o objeto JSON de params como
# texto, preservando tipos/aninhamento/nulos (a cópia fiel). O mapa consultável é
# derivado dele em construir_bronze.
SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("user_pseudo_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_hint", StringType()),
    StructField("page", PAGE),
    StructField("traffic", TRAFFIC),
    StructField("device", DEVICE),
    StructField("params", StringType()),
    StructField("items", ArrayType(ITEM)),
])

# Nome da coluna temporária com o params cru lido do JSON (vira params_json).
_COL_PARAMS_BRUTO = "params"
# Nomes das colunas de params na Bronze gravada.
COL_PARAMS_JSON = "params_json"          # objeto JSON cru, fiel (R4)
COL_PARAMS = "params"                    # map<string,string> derivado, consultável
TIPO_PARAMS_MAP = "map<string,string>"

# Nomes das colunas de proveniência (R4).
COL_ARQUIVO_ORIGEM = "_arquivo_origem"
COL_INGERIDO_EM = "_ingerido_em"


def desdobrar_params(landing: DataFrame) -> DataFrame:
    """Transforma o params cru (string) em duas colunas, sem perder nada.

    - ``params_json`` (string): o objeto JSON de params como texto — a cópia fiel
      (R4), com tipos, aninhamento, nulos e chaves inesperadas preservados (o
      Spark pode normalizar o espaçamento ao reserializar).
    - ``params`` (map<string,string>): derivado de ``params_json`` com
      ``from_json``, para consultar as chaves no explorador e alimentar a Silver.

    Uma única leitura da landing alimenta as duas (o schema lê params como texto).
    """
    return (
        landing
        .withColumnRenamed(_COL_PARAMS_BRUTO, COL_PARAMS_JSON)
        .withColumn(COL_PARAMS, F.from_json(F.col(COL_PARAMS_JSON), TIPO_PARAMS_MAP))
    )


def adicionar_proveniencia(landing: DataFrame) -> DataFrame:
    """Acrescenta as colunas de proveniência exigidas pela R4.

    - ``_arquivo_origem``: o arquivo da landing de onde a linha veio
      (``input_file_name()`` do Spark).
    - ``_ingerido_em``: o instante (UTC) em que a linha foi lida pela Bronze
      (``current_timestamp()``, igual para todas as linhas de uma execução).
    """
    return (
        landing
        .withColumn(COL_ARQUIVO_ORIGEM, F.input_file_name())
        .withColumn(COL_INGERIDO_EM, F.current_timestamp())
    )


def construir_bronze(spark: SparkSession) -> DataFrame:
    """Lê a landing com schema explícito e devolve o DataFrame da Bronze.

    Falha com mensagem clara se a landing não existir (R2). Não aplica nenhuma
    regra de limpeza ou rejeição: isso é da Silver. A proveniência é calculada
    sobre a leitura crua (``input_file_name`` antes de qualquer transformação)."""
    exigir_camada(spark, glob_eventos("landing"), "python gerador/gerar_eventos.py")
    landing = ler_eventos(spark, "landing", "json", schema=SCHEMA)
    return desdobrar_params(adicionar_proveniencia(landing))


def main() -> int:
    spark = criar_sessao("bronze")
    try:
        bronze = construir_bronze(spark)
        lidos = bronze.count()
        gravar_eventos(bronze, "bronze", "append")
        titulo("Bronze")
        print(f"[ok] Landing lida e gravada na Bronze (append) por s3a: {lidos} linhas nesta execução.")

        total = ler_eventos(spark, "bronze")
        print(f"[ok] Total acumulado na Bronze (todas as execuções): {total.count()} linhas.")
        print("Linhas por event_name na Bronze:")
        total.groupBy("event_name").count().orderBy(F.desc("count")).show(25, truncate=False)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
