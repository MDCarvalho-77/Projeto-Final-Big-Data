# Tecnologia, ambiente e restrições

Fontes: `SETUP.md`, `Dockerfile`, `docker-compose.yml`, `requirements.txt`,
`pipeline/comum.py`, `docs/ENUNCIADO.md` (R1–R4).

## Execução: sempre via Docker Compose

Nada é instalado na máquina além do Docker. Todo comando Python do projeto roda
dentro do contêiner `app`:

```bash
docker compose run --rm app python <script.py>
docker compose run --rm app python -m pytest
```

Não executar os jobs do pipeline fora do contêiner. A imagem traz os jars do
conector `s3a` e a configuração correta do Spark.

## Serviços (docker-compose.yml)

| Serviço | Imagem | Papel |
| --- | --- | --- |
| `floci` | `floci/floci:latest` | S3 emulado em `http://floci:4566` (`localhost:4566` no host). Guarda os dados **em memória**: ao reiniciar o Docker, o bucket fica vazio e é preciso rodar o gerador de novo |
| `app` | construída pelo `Dockerfile` | Python 3.11, Java 17, PySpark 4.2.0, conector `s3a`; onde o pipeline roda |
| `explorador` | mesma imagem do `app` | console Streamlit em `http://localhost:8501` (S3, Glue Catalog, Jobs ETL, Workflows, Athena SQL) |

Variáveis de ambiente do S3 (definidas no compose): `AWS_ENDPOINT_URL=http://floci:4566`,
`AWS_S3_BUCKET=datalake-tracking`, chaves `test`/`test`, região `us-east-1`.

O repositório é montado em `/projeto` dentro dos contêineres — edições no host
valem na hora, sem reconstruir a imagem.

## Versões fixadas (requirements.txt)

- `pyspark==4.2.0` (Hadoop 3.5.0 embutido) e `py4j==0.10.9.9`
- `numpy==2.4.6`, `pandas==2.3.3` (pandas fica em 2.x: o PySpark 4.2 ainda não
  suporta totalmente o pandas 3), `pyarrow==25.0.1`
- `boto3==1.43.111` — usado **apenas pelo gerador**, nunca pelo pipeline (R1)
- `pytest==9.1.1`, `duckdb==1.5.6`, `streamlit==1.65.0`

Conector S3 (instalado no Dockerfile): `hadoop-aws` 3.5.0, AWS SDK v2 bundle
2.35.4, `analyticsaccelerator-s3` 1.3.1, mais o binder `log4j-slf4j-impl` 2.25.4.

## Arquitetura de armazenamento

Bucket único `datalake-tracking`, tudo em Parquet nas camadas analíticas:

```text
s3://datalake-tracking/
├── landing/   eventos brutos JSON Lines (já existe; só leitura)
├── bronze/
├── silver/
└── gold/
```

Layout de partições (landing, Bronze e Silver — direto na raiz da camada, sem
pasta `eventos`):

```text
<camada>/event_name=<nome>/year=AAAA/month=MM/day=DD/...
silver/itens/year=AAAA/month=MM/day=DD/...   (vizinha das partições event_name=)
silver/eventos_rejeitados/...                (vizinha)
gold/<nome da tabela>/...
```

`month` e `day` sempre com dois dígitos. A inferência de tipo de partição fica
**desligada** na SparkSession para `month=09` não virar `month=9`.

## Restrições técnicas obrigatórias (R1–R4)

- **Acesso ao S3 só pelo Spark**, com caminhos `s3a://datalake-tracking/...`
  (`spark.read` / `df.write`).
- **Proibido `boto3`** ou qualquer SDK de nuvem no pipeline. Proibido baixar
  arquivos para o disco local (exceção: o `site/mapa_calor.js` do R11, gravado no
  fim do job de exportação, após ler a Gold pelo Spark).
- **Parquet** em todas as camadas analíticas.
- **Bronze append-only** (`mode("append")`), com **schema explícito** (sem
  inferência), partições `event_name/year/month/day` e colunas de proveniência
  (arquivo de origem + instante de ingestão).
- **Silver** grava com overwrite **dinâmico** (`partitionOverwriteMode=dynamic`)
  para não apagar `itens/` e `eventos_rejeitados/`.

## Reutilizar `pipeline/comum.py` (obrigatório)

Importar, nunca reescrever. Funções disponíveis:

- `criar_sessao(nome)` — SparkSession configurada para o floci (s3a), UTC e
  partição como texto.
- `caminho(camada, bucket=None)` — monta o `s3a://...`.
- `glob_eventos(camada)` — `"<camada>/event_name=*"`.
- `ler_eventos(spark, camada, formato="parquet", schema=None)` — lê só as pastas
  `event_name=` com `basePath`, devolvendo `event_name/year/month/day` como
  colunas. Para a landing: `ler_eventos(spark, "landing", "json", schema=SCHEMA)`.
- `gravar_eventos(df, camada, modo)` — grava particionado, overwrite dinâmico.
- `exigir_camada(spark, glob_eventos("bronze"), "python pipeline/bronze.py")` —
  para o job com mensagem clara se a dependência não existe (R2).
- `apagar_camada(spark, camada)` — limpa a camada para recomeçar.
- `arquivo_do_projeto("site/mapa_calor.js")` — caminho no repositório montado em
  `/projeto` (use em vez de `Path(__file__)`).
- `titulo(texto)` — título destacado na saída.

## Spark 4: modo ANSI

O PySpark 4.2 liga o modo ANSI por padrão: divisão por zero e cast inválido
**param o job com erro** em vez de devolver `null`. Quando o `null` é desejado
(ex.: rejeitar dado sujo na Silver, taxas com denominador zero na Gold), usar as
funções `try_`: `F.try_divide`, `F.col(...).try_cast(...)`,
`F.try_to_timestamp(...)`.
