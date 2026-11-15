# Entrega 01 — Bronze

## Objetivo e escopo

Ingestão dos eventos brutos da `landing/` para a camada `bronze/`, append-only,
com schema explícito, partições e proveniência. Requisitos: **R1, R2, R4**.

## Arquivos oficiais de código relacionados

- `pipeline/bronze.py` — job Bronze (implementado).
- `pipeline/comum.py` — `criar_sessao`, `ler_eventos`, `gravar_eventos`,
  `exigir_camada`, `glob_eventos` (reutilizados, não reescritos).
- `tests/pipeline/test_bronze.py` — testes do job Bronze (14 testes).
- Fontes de requisito: `docs/ENUNCIADO.md` (R1, R2, R4), `docs/CONTRATO_EVENTO.md`
  (formato do evento e layout da landing).

## Decisões técnicas

- **Schema explícito (R4), sem inferência**, fiel ao contrato: campos simples;
  `page`/`traffic`/`device` como `struct`; `items` como `array<struct>`.
- **`params` em duas colunas** (decisão revisada em 25/10/2026 — ver
  `evidencias/05_decisao_params.md`):
  - `params_json` (string): o objeto JSON de `params` preservando tipos, nulos e
    aninhamento — a cópia fiel (R4).
  - `params` (`map<string,string>`): derivado de `params_json`, consultável no
    explorador e base para a Silver (R6).
  - Alternativa `VariantType` foi testada (lê/grava/relê no Spark e no DuckDB) e
    funciona, mas foi preterida por ser mais nova e mexer mais na camada/catálogo;
    `params_json` + `map` já é fiel com a menor alteração.
- `event_ts` fica como **texto** na Bronze (cópia fiel): há eventos com ts nulo ou
  no futuro; converter aqui, no modo ANSI do Spark 4, pararia o job. Tipagem é da
  Silver.
- `event_name` vem **da partição** do caminho (não do schema), evitando coluna
  duplicada.
- **Proveniência (R4):** `_arquivo_origem` (`input_file_name()`) e `_ingerido_em`
  (`current_timestamp()`, UTC).
- **Gravação (R1, R4):** Parquet em `s3a://datalake-tracking/bronze/`,
  particionado por `event_name/year/month/day`, modo **append** (append-only).
- **Sem limpeza:** nenhuma dedup/validação/rejeição (isso é da Silver).

## Evidências (execução real, 2026-10-25)

Em `evidencias/`:

- `01_execucao_bronze.txt` — saída real de `pipeline/bronze.py` (64372 linhas,
  total acumulado 64372 — sem duplicação).
- `02_schema_proveniencia_particoes.txt` — schema (com `params_json` + `params`),
  proveniência (64372/64372; 1381 arquivos de origem) e partições
  `event_name/year/month/day` com `month=09` texto.
- `03_testes.txt` — 21 testes do pipeline e 294 na suíte completa, todos passando.
- `04_ambiente.txt` — floci `(healthy)`, landing gerada e 3 linhas `[ok]`.
- `05_decisao_params.md` — a decisão técnica sobre `params` e as alternativas
  testadas.
- `06_catalogo_athena.txt` — Data Catalog e Athena validados pela via real do
  explorador (descoberta de `bronze.eventos`, DESCRIBE, execução dos `.sql`).

Capturas de tela do explorador (adicionadas em 25/10/2026, PNG reais):

- `s3_bronze_particoes.png.png` — S3 · Buckets → `bronze` → Partições
  (`event_name/year/month/day`; 327 partições, 993 arquivos, 13,7 MB).
- `catalog_bronze_schema.png.png` e `catalog_bronze_schema2.png.png` —
  Glue · Data Catalog → `bronze.eventos` → Esquema, mostrando `params_json`
  (varchar), `params` (map(varchar,varchar)), os `struct` aninhados, a proveniência
  e as 4 chaves de partição.
- `athena_contagem.png.png` — Athena executando a contagem por evento (Concluída,
  23 linhas).
- `athena_consulta_bronze.png.png` — Athena executando `SELECT * FROM
  bronze.eventos LIMIT 10` (Concluída, 10 linhas).

Observação: os arquivos têm extensão dupla `.png.png` (como foram salvos); são PNG
válidos (verificados por assinatura e dimensões).

## Consultas e resultados

- `consultas/contagem_por_evento.sql`, `consultas/proveniencia_e_particoes.sql`,
  `consultas/params_fidelidade.sql`.
- `resultados/contagem_por_evento.csv`, `resultados/proveniencia.csv`,
  `resultados/particoes_amostra.csv`, `resultados/params_fidelidade.csv` — CSVs
  reais, exportados pelo motor da tela Athena (DuckDB sobre o floci).

## Critérios de conclusão

- [x] Código da Bronze implementado (`pipeline/bronze.py`).
- [x] Questão do `params` resolvida com testes reproduzíveis (map + params_json).
- [x] Testes executados e registrados (21 no pipeline; 294 na suíte completa).
- [x] Execução integrada real no contêiner `app` (landing → Bronze), sem
      duplicação (append total = 64372).
- [x] Estrutura gravada confere com o contrato e o particionamento (schema
      explícito, proveniência, `event_name/year/month/day`, Parquet, append-only).
- [x] Data Catalog e Athena validados pela via real do explorador.
- [x] Consultas SQL de validação executadas e CSVs reais exportados.
- [x] **Capturas de tela** do explorador (S3, Data Catalog, Athena) anexadas em
      `evidencias/` (5 PNG reais).

## Estado atual

- **Implementado:** sim (`pipeline/bronze.py`, com a decisão de `params` revisada).
- **Testado:** sim (21 no pipeline, 294 na suíte; nenhuma regressão).
- **Validado em execução integrada:** sim (Bronze real no floci; schema,
  proveniência e partições conferidos; Data Catalog e Athena via explorador).
- **Evidências capturadas:** registros de console, SQL, CSVs reais **e as 5
  capturas de tela do explorador** (S3, Data Catalog e Athena).
- **Pendência:** nenhuma. A etapa está **validada** e pronta para entrega.
