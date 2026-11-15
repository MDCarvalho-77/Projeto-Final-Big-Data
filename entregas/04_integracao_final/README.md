# Entrega 04 — Integração final

## Objetivo e escopo

Amarrar o pipeline inteiro: o script de sequência que roda os oito jobs na ordem
exigida (R3), a reconstrução do datalake do zero a partir da landing, os testes
integrados e o relatório de negócio. Requisitos: **R3** e a entrega do
`RELATORIO.md` (ENUNCIADO seção 3).

## Arquivos oficiais de código relacionados

- `pipeline/executar_pipeline.py` — script de sequência dos 8 jobs (a implementar;
  **não gera dados**, R3).
- Os oito jobs de `pipeline/` (Bronze, Silver, 5 Gold, exportação do mapa).
- `pipeline/README.md` — atualizar para documentar como rodar o pipeline completo.
- `RELATORIO.md` — na **raiz do projeto** (a escrever).
- `infra/verificar_ambiente.py` — validação do ambiente (pronto).
- Fontes: `docs/ENUNCIADO.md` (R3, seções 1, 3, 7).

## Evidências obrigatórias

- Log real da reconstrução completa (`python pipeline/executar_pipeline.py`) a
  partir da landing, do zero.
- Saída de `docker compose run --rm app python -m pytest` (suíte completa).
- As três linhas `[ok]` de `infra/verificar_ambiente.py`.
- Confirmação visual do mapa de calor aceso no site.

## Consultas e resultados a registrar

- As consultas que sustentam as quatro respostas do `RELATORIO.md`.
- CSVs reais consolidados em `resultados/`, com os números citados no relatório.
- Os números da seção "Qualidade do dado" (duplicatas, rejeições por motivo,
  armadilhas).

## Critérios de conclusão

- Um único comando reconstrói o datalake a partir da landing (R3).
- Pipeline completo roda sem erro; testes passam.
- `RELATORIO.md` responde às quatro perguntas, cada uma com tabela + número +
  recomendação, e traz a seção "Qualidade do dado".
- Checklist final da rubrica (seção 7) revisado.
- Evidências capturadas nesta pasta.

## Estado atual

**Validada.** A reconstrução completa do datalake a partir da landing roda com um
único comando (`pipeline/executar_pipeline.py`, rc=0) e não gera dados (R3).

Evidências nesta pasta (`evidencias/`):

- `verificar_ambiente.txt` — as três linhas `[ok]` de `infra/verificar_ambiente.py`
  (landing lida por s3a com 64.372 linhas; escrita/leitura de Parquet; limpeza).
- `execucao_pipeline.txt` — log real dos 8 jobs na ordem oficial, todos `[ok]`:
  bronze → silver → gold_funil → gold_engajamento → gold_receita_aquisicao →
  gold_ml → gold_mapa_calor → exportar_mapa_calor. Comprova a estratégia Bronze
  **append-only** (modo `auto` pula a ingestão quando a Bronze já existe) e a
  geração de `site/mapa_calor.js`.
- `pytest_suite.txt` — suíte completa: **436 passed** (347 existentes + novos).
- `validacao_gold.txt` — leitura das 15 tabelas de `gold/` pelo Spark, contagens e
  todas as invariantes conferidas.

Volume de referência (seed 42): Bronze 64.372 → Silver 61.813 válidos / 656
duplicatas / 1.903 rejeitados / 24.427 itens → 8.793 sessões. O `RELATORIO.md`
(raiz do projeto) responde às quatro perguntas de negócio com números rastreáveis
aos CSVs de `entregas/03_gold/.../resultados/` e traz a seção "Qualidade do dado".
