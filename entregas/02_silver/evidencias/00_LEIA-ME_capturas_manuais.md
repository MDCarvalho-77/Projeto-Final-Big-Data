# Evidências da Silver — o que está capturado e o que falta

## Capturado automaticamente (execução real, não fictícia)

Os arquivos de texto nesta pasta foram transcritos de execuções reais no
contêiner `app`/`floci` em 2026-10-27:

- `01_execucao_silver.txt` — saída real do job `pipeline/silver.py` (válidos
  61813, duplicatas 656, rejeitados 1903, itens 24427) e a reconciliação [ok].
- `02_schema_particoes.txt` — schema, tipos (R6), partições, R5/R8/R9 e
  integridade referencial itens↔eventos, lidos da Silver gravada pelo Spark.
- `03_testes.txt` — resultado real do `pytest`: 53 (Silver), 74 (pipeline),
  347 (suíte completa; 294 antes + 53 novos, sem regressão).
- `04_idempotencia.txt` — três execuções com contagens idênticas e a Bronze
  preservada (64372 antes e depois).
- `05_catalogo_athena.txt` — Data Catalog e Athena validados pela VIA REAL do
  explorador (descoberta de `silver.eventos`, `silver.eventos_rejeitados`,
  `silver.itens`, DESCRIBE do esquema e execução das quatro consultas `.sql`).

As consultas SQL de validação foram executadas pelo motor da tela Athena do
explorador (DuckDB sobre o floci) e exportadas como CSVs reais em
`../resultados/` (`validos_resumo.csv`, `rejeitados_por_motivo.csv`,
`sessoes_por_usuario.csv`, `itens_por_produto.csv`).

## Falta captura manual (telas do explorador, imagens .png)

Os registros de console acima **não substituem** os prints da interface. As
capturas exigem o navegador e não podem ser geradas por linha de comando. Para
capturá-las, com o ambiente de pé:

```bash
docker compose up -d floci explorador
```

Abra http://localhost:8501, clique em **⟳ Sincronizar** na tela Jobs ETL (para o
explorador enxergar o `pipeline/`), e capture, salvando cada arquivo nesta pasta
(`entregas/02_silver/evidencias/`):

1. **S3 · Buckets** → `datalake-tracking` → pasta `silver/`: a árvore com as
   partições `event_name=.../year=2026/month=09/day=...` ao lado das pastas
   `itens/` e `eventos_rejeitados/`.
   Salvar como: `s3_silver_particoes.png`.
2. **Glue · Data Catalog** → banco `silver` → tabela `eventos` → aba **Esquema**:
   as colunas tipadas de `params` (`value` DOUBLE, `percent_scrolled`/`video_percent`
   INTEGER, `outbound` BOOLEAN, `element_id` VARCHAR), `event_ts_ts` TIMESTAMP,
   `session_id`, `user_id_resolvido`, `pagina`, e as 4 chaves de partição.
   Salvar como: `catalog_silver_eventos_schema.png`.
3. **Glue · Data Catalog** → banco `silver` → tabelas `eventos_rejeitados` e
   `itens` → aba **Esquema**: `motivo_rejeicao` em rejeitados; `item_id`/`price`/
   `quantity` + vínculo `event_id`/`session_id` em itens.
   Salvar como: `catalog_silver_itens_rejeitados_schema.png`.
4. **Athena · Editor** → rodar `consultas/rejeitados_por_motivo.sql` e capturar a
   faixa verde (status, 6 linhas) com a tabela dos 6 motivos.
   Salvar como: `athena_rejeitados_por_motivo.png`.
5. (Recomendado) **Athena · Editor** → rodar `consultas/sessoes_por_usuario.sql`
   e `consultas/itens_por_produto.sql`, capturando as tabelas.
   Salvar como: `athena_sessoes.png` e `athena_itens.png`.

Os números que devem aparecer nessas telas são exatamente os dos CSVs em
`../resultados/` (ex.: 6 motivos R7; 1903 rejeitados no total; 24427 linhas de
itens; usuários com 1 a 5 sessões). A conferência não visual desses mesmos fatos
está em `05_catalogo_athena.txt`.
