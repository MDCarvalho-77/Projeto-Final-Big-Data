# Entrega 02 — Silver

## Objetivo e escopo

Transformar a Bronze em dado limpo: deduplicação, extração tipada de `params`,
validação com motivo de rejeição, sessionização, identity stitching e tabela de
itens explodida. Requisitos: **R5, R6, R7, R8, R9, R10**.

## R5–R10 implementados

- **R5 — deduplicação por `event_id`.** `row_number()` com desempate estável
  (`_arquivo_origem → _ingerido_em → event_ts → event_id`), uma cópia por
  `event_id`. Linhas sem `event_id` são separadas antes da janela e mantidas
  isoladas (não viram duplicata). **656** duplicatas removidas (64372 → 63716).
- **R6 — tipagem dos `params`.** Colunas tipadas a partir do `params_json` cru
  (número/booleano/nulo fiéis), via `from_json` com schema único de união; inclui
  `element_id` (opcional, nunca rejeita). `from_json`/`get_json_object` fazem
  *parse* (não cast ANSI); casts posteriores usam `try_cast`/`try_to_timestamp`.
  Flags de presença distinguem chave ausente (ausência legítima) de valor inválido.
- **R7 — validação com motivo de rejeição.** As 6 regras mínimas do enunciado +
  2 acrescentadas (`valor_nao_numerico`, `marco_nao_numerico`). Dois passos
  (regras independentes; depois `reembolso_sem_compra` contra as compras válidas).
  Um motivo por linha, rótulos constantes. **1903** eventos rejeitados.
- **R8 — sessionização.** `session_id` por corte estrito de **> 30 min** por
  `user_pseudo_id`, window functions, ordem `(event_ts_ts, event_id)`, **sem**
  `session_hint`. **8793** sessões para **5994** usuários.
- **R9 — identity stitching.** `user_id_resolvido` por `user_pseudo_id`, lido do
  mapa de todos os eventos pós-dedup e aplicado inclusive aos eventos anteriores
  ao login. **8736** eventos receberam `user_id` por stitching.
- **R10 — tabela de itens.** `items[]` explodido (`F.explode`), uma linha por item
  por evento, vinculada ao evento de origem. **24427** linhas.

## Arquivos oficiais de código e testes

- `pipeline/silver.py` — job Silver (implementado).
- `pipeline/comum.py` — `criar_sessao`, `ler_eventos`, `gravar_eventos`,
  `exigir_camada`, `glob_eventos`, `caminho` (reutilizados, não reescritos).
- `tests/pipeline/test_silver.py` — testes do job Silver (53 testes).
- Fontes de requisito: `docs/ENUNCIADO.md` (R5–R10, seção 5.4),
  `docs/CONTRATO_EVENTO.md` (tabela de `params` por evento, `element_id`,
  catálogo de eventos).
- Design e auditoria: `.agents/tasks/silver-entrega-02/design.md` e `audit.md`.

## Decisões de qualidade

- **Dedup determinística** com desempate estável; linhas sem `event_id` nunca
  colapsam (cada uma é um evento). Proveniência da cópia escolhida preservada.
- **Tipagem a partir do `params_json`** (não do `map`), para não perder a etiqueta
  de tipo JSON. `"value": null` conta como ausência (não há quantia a validar).
- **Modo ANSI respeitado:** `try_to_timestamp`/`try_cast`; o `null` de conversão é
  sinal de defeito classificado (R7), nunca mascarado.
- **Um motivo por linha** nos rejeitados; rótulos constantes para agregação limpa.
  Dados originais (`params_json`, proveniência, `event_ts` cru + tipado) preservados
  para rastreabilidade e correção da origem.
- **Reconciliação verificada em `event_id` distintos** no próprio job, que aborta
  se qualquer identidade falhar.
- **Gravação segura:** três prefixos disjuntos, overwrite dinâmico nos
  particionados e total nos rejeitados; nunca grava na raiz `silver/` direta nem
  usa `apagar_camada("silver")`. Idempotente sobre a mesma Bronze.

## Evidências (execução real, 2026-10-27)

Em `evidencias/`:

- `01_execucao_silver.txt` — saída real de `pipeline/silver.py` e a reconciliação.
- `02_schema_particoes.txt` — schema, tipos (R6), partições, R5/R8/R9 e
  integridade referencial itens↔eventos, lidos da Silver gravada pelo Spark.
- `03_testes.txt` — 53 (Silver), 74 (pipeline), 347 (suíte completa), sem regressão.
- `04_idempotencia.txt` — três execuções idênticas; Bronze preservada (64372).
- `05_catalogo_athena.txt` — Data Catalog e Athena pela via real do explorador
  (descoberta de `silver.eventos`/`eventos_rejeitados`/`itens`, DESCRIBE, as 4
  consultas).
- `00_LEIA-ME_capturas_manuais.md` — guia das capturas (mantido como referência).

Capturas de tela do explorador (PNG reais, adicionadas em 2026-11-01). Os nomes
são os originais do Windows ("Captura de tela AAAA-MM-DD HHMMSS.png") e foram
preservados:

- `Captura de tela 2026-11-01 010442.png` — **S3 · Buckets** → `datalake-tracking`
  → `silver/`: as partições `event_name=...` ao lado de `eventos_rejeitados/` e
  `itens/` (24 objetos).
- `Captura de tela 2026-11-01 011647.png` — **Data Catalog** → `silver.eventos` →
  Esquema (parte 1): visão geral (parquet, chaves `event_name/year/month/day`,
  2297 arquivos) e as primeiras colunas.
- `Captura de tela 2026-11-01 011732.png` — **Data Catalog** → `silver.eventos` →
  Esquema (parte 2): `params_json` (varchar), `params` (map), `event_ts_ts`
  (timestamp) e as colunas tipadas de R6 (`percent_scrolled`/`video_percent`
  integer, `outbound` boolean, `video_duration` double…).
- `Captura de tela 2026-11-01 011808.png` — **Data Catalog** → `silver.eventos` →
  Esquema (parte 3): `value`/`tax`/`shipping` double, `transaction_id`,
  `element_id`, flags de presença (`tem_value`…), `pagina`, `session_id`,
  `user_id_resolvido` e as 4 chaves de partição marcadas.
- `Captura de tela 2026-11-01 011919.png` — **Data Catalog** →
  `silver.eventos_rejeitados` → Esquema (`motivo_rejeicao`, proveniência,
  `event_ts`/`event_ts_ts`, `params_json`; 8 arquivos).
- `Captura de tela 2026-11-01 012104.png` — **Data Catalog** → `silver.itens` →
  Esquema (`item_id`/`item_name`/`item_category`/`price`/`quantity`, vínculo
  `event_id`/`session_id`/`user_id_resolvido`; chaves `year/month/day`).
- `Captura de tela 2026-11-01 012357.png` — **Athena** → `rejeitados_por_motivo.sql`:
  Concluída, 6 linhas (timestamp 948, valor 349, marco 272, fora_catalogo 271,
  reembolso 32, transacao_sem_id 31).
- `Captura de tela 2026-11-01 012556.png` — **Athena** → `sessoes_por_usuario.sql`:
  Concluída, 5 linhas (1→3966, 2→1443, 3→401, 4→182, 5→2 usuários).
- `Captura de tela 2026-11-01 012709.png` — **Athena** → `itens_por_produto.sql`:
  Concluída, 4 linhas (PLANO-PRO 8756, PLANO-BASICO 7895, PLANO-PREMIUM 7002,
  BUMP-MENTORIA 774).
- `Captura de tela 2026-11-01 012838.png` — **Glue · Jobs ETL** → job `silver` em
  execução (log do Spark).
- `Captura de tela 2026-11-01 012942.png` — **Glue · Jobs ETL** → log completo do
  `silver` com as contagens e as 5 identidades de reconciliação `[ok]`.
- `Captura de tela 2026-11-01 013052.png` — **Glue · Jobs ETL** → job `silver`
  **✓ Sucesso** (código de saída 0, 130 s), log completo incl. `[ok] Gravados:
  silver/eventos_rejeitados, silver/itens, silver/event_name=...`.
- `Captura de tela 2026-11-01 011324.png` — **S3 · Buckets** (mesma listagem do
  `silver/`) exibindo o banner transitório de renderização do Streamlit
  diagnosticado (frontend; não afeta os dados nem as demais telas).

## Consultas e resultados

- `consultas/validos_resumo.sql`, `consultas/rejeitados_por_motivo.sql`,
  `consultas/sessoes_por_usuario.sql`, `consultas/itens_por_produto.sql`.
- `resultados/validos_resumo.csv`, `resultados/rejeitados_por_motivo.csv`,
  `resultados/sessoes_por_usuario.csv`, `resultados/itens_por_produto.csv` — CSVs
  reais, exportados pelo motor da tela Athena (DuckDB sobre o floci).
- `resultados/RESUMO.md` — os números reais consolidados, rastreáveis a cada CSV.

## Resultados/métricas reais observados

| Métrica | Valor |
| --- | --- |
| Processados (Bronze) | 64372 |
| Duplicatas removidas (R5) | 656 |
| Eventos únicos (pós-dedup) | 63716 |
| Válidos | 61813 |
| Rejeitados (eventos distintos) | 1903 |
| Itens explodidos (R10) | 24427 |
| Sessões distintas (R8) | 8793 |
| Stitching aplicado (R9) | 8736 |

Rejeitados por motivo: timestamp 948, valor 349, marco 272, fora_catalogo 271,
reembolso_orfao 32, transacao_sem_id 31. Reconciliação: 61813 + 656 + 1903 = 64372.

## Critérios de conclusão

- [x] Cada `event_id` único nos válidos (R5); 656 duplicatas removidas.
- [x] `params`/`element_id` em colunas tipadas a partir do `params_json` (R6).
- [x] `silver/eventos_rejeitados/` com um motivo por linha, cobrindo as 6 regras (R7).
- [x] Sessões por corte de > 30 min, sem `session_hint` (R8).
- [x] Stitching aplicado a eventos do `user_pseudo_id`, inclusive anteriores ao
      login (R9).
- [x] `silver/itens/` explodida, 0 órfãos, Σ size(items) == contagem (R10).
- [x] Gravação com overwrite dinâmico, preservando `itens/` e `eventos_rejeitados/`;
      Bronze intacta; idempotência comprovada.
- [x] Testes do job passando (53); suíte completa 347 sem regressão.
- [x] Consultas SQL executadas pela via real do explorador e CSVs reais exportados.
- [x] Capturas de tela do explorador (S3, Data Catalog das 3 tabelas, 3 consultas
      Athena e o job `silver` com Sucesso) anexadas em `evidencias/` (13 PNG reais).

## Estado atual

- **Implementado:** sim (`pipeline/silver.py`).
- **Testado:** sim (53 do job; 74 no pipeline; 347 na suíte, sem regressão).
- **Validado em execução integrada:** sim (Silver real no floci sobre a Bronze
  real; schema, tipos, partições, integridade referencial e reconciliação
  conferidos; idempotência comprovada; Bronze preservada).
- **Evidências capturadas:** registros de console, SQL e CSVs reais, catálogo e
  Athena pela via real do explorador, **e as 13 capturas de tela do explorador**
  (S3, Data Catalog das 3 tabelas, 3 consultas Athena e o job `silver` com Sucesso).
- **Pendência:** nenhuma. A etapa está completa.

**Validada:** testes, execução integrada, idempotência e as evidências visuais do
explorador, todos comprovados com evidência real.
