# Evidências da Bronze — o que está capturado e o que falta

## Capturado automaticamente (execução real, não fictícia)

Os arquivos de texto nesta pasta foram transcritos de execuções reais no
contêiner `app`/`floci` em 2026-10-25 (retomada; a decisão de `params` foi
revisada e a Bronze reexecutada):

- `01_execucao_bronze.txt` — saída real do job `pipeline/bronze.py` (64372 linhas).
- `02_schema_proveniencia_particoes.txt` — schema (com `params_json` + `params`),
  proveniência (R4) e partições (R4) lidos da Bronze gravada, pelo Spark.
- `03_testes.txt` — resultado real do `pytest` (21 testes do pipeline; 294 na
  suíte completa).
- `04_ambiente.txt` — floci saudável, geração da landing e as três linhas `[ok]`
  de `infra/verificar_ambiente.py`.
- `05_decisao_params.md` — a decisão técnica sobre `params` (map + params_json),
  com as alternativas testadas (inclusive VARIANT) e seus limites.
- `06_catalogo_athena.txt` — Data Catalog e Athena validados pela VIA REAL do
  explorador (descoberta de `bronze.eventos`, DESCRIBE do esquema e execução das
  três consultas `.sql`).

As consultas SQL de validação foram executadas pelo motor da tela Athena do
explorador (DuckDB sobre o floci) e exportadas como CSVs reais em
`../resultados/` (`contagem_por_evento.csv`, `proveniencia.csv`,
`particoes_amostra.csv`, `params_fidelidade.csv`).

## Falta captura manual (telas do explorador, imagens .png)

Os registros de console acima **não substituem** os prints da interface exigidos
pela entrega. As capturas exigem o navegador e não podem ser geradas por linha de
comando. Para capturá-las, com o ambiente de pé:

```bash
docker compose up -d floci explorador
```

Abra http://localhost:8501, clique em **⟳ Sincronizar** na tela Jobs ETL (para o
explorador enxergar o `pipeline/`), e capture, salvando cada arquivo nesta pasta
(`entregas/01_bronze/evidencias/`):

1. **S3 · Buckets** → `datalake-tracking` → pasta `bronze/`: a árvore de
   partições `event_name=.../year=2026/month=09/day=.../part-*.parquet`.
   Salvar como: `s3_bronze_particoes.png`.
2. **Glue · Data Catalog** → banco `bronze` → tabela `eventos` → aba **Esquema**:
   as colunas e os tipos — `page`/`traffic`/`device` como `struct`, `items` como
   `struct[]`, **`params_json` como string**, **`params` como `map`**,
   `_arquivo_origem`, `_ingerido_em`, e as chaves de partição
   `event_name/year/month/day`. Salvar como: `catalog_bronze_schema.png`.
3. **Athena · Editor** → rodar `consultas/contagem_por_evento.sql` e capturar a
   faixa verde (status, linhas, dados lidos) com a tabela.
   Salvar como: `athena_contagem.png`.
4. (Recomendado) **Athena · Editor** → rodar `consultas/params_fidelidade.sql` e
   capturar a tabela mostrando `params_json` com os tipos preservados.
   Salvar como: `athena_params.png`.

Os números que devem aparecer nessas telas são exatamente os dos CSVs em
`../resultados/` (ex.: 64372 linhas no total; `add_to_wishlist` presente;
`bronze.eventos` com `params_json` e `params` no esquema). A conferência não
visual desses mesmos fatos está em `06_catalogo_athena.txt`.
