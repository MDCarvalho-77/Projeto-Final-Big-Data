# Resumo dos números reais da Silver (seed 42)

Execução real de `pipeline/silver.py` sobre a Bronze real do floci, em 2026-10-27.
Cada número abaixo é rastreável ao log do job (`../evidencias/01_execucao_silver.txt`)
e/ou a uma consulta em `../consultas/` com o CSV ao lado neste diretório.

## Reconciliação (R5/R7)

| Métrica | Valor | Origem |
| --- | --- | --- |
| Processados (Bronze) | 64372 | log do job; Bronze preservada |
| Duplicatas removidas (R5) | 656 | log do job |
| Eventos únicos (pós-dedup) | 63716 | 64372 − 656 |
| Válidos | 61813 | `validos_resumo.csv` (Σ), log do job |
| Rejeitados (eventos distintos) | 1903 | `rejeitados_por_motivo.csv` (Σ), log do job |
| Itens explodidos (R10) | 24427 | `itens_por_produto.csv` (Σ), log do job |

Identidade: 61813 + 656 + 1903 = 64372. Interseção válidos ∩ rejeitados = 0.

## Rejeitados por motivo (R7) — `rejeitados_por_motivo.csv`

| Motivo | Linhas = eventos distintos |
| --- | --- |
| timestamp_ausente_ou_futuro | 948 |
| valor_monetario_invalido | 349 |
| marco_fora_do_padrao | 272 |
| evento_fora_catalogo | 271 |
| reembolso_sem_compra | 32 |
| transacao_sem_id | 31 |

As 6 regras mínimas do R7 estão cobertas. Nenhum evento teve mais de um motivo
nesta execução, logo linhas == eventos distintos (1903).

## Sessões (R8) — `sessoes_por_usuario.csv`

| Sessões por usuário | Usuários |
| --- | --- |
| 1 | 3966 |
| 2 | 1443 |
| 3 | 401 |
| 4 | 182 |
| 5 | 2 |

Total: 5994 `user_pseudo_id`, 8793 sessões distintas. Usuários com mais de uma
sessão comprovam os cortes de inatividade > 30 min (sessão calculada, sem
`session_hint`).

## Identity stitching (R9)

Eventos com `user_id` original nulo mas `user_id_resolvido` preenchido: **8736**
(o `user_id` do login/sign_up aplicado a eventos anteriores ao login).

## Itens por produto (R10) — `itens_por_produto.csv`

| item_id | categoria | preço | linhas | Σ quantidade |
| --- | --- | --- | --- | --- |
| PLANO-PRO | plano | 597,00 | 8756 | 8756 |
| PLANO-BASICO | plano | 297,00 | 7895 | 7895 |
| PLANO-PREMIUM | plano | 997,00 | 7002 | 7002 |
| BUMP-MENTORIA | order_bump | 147,00 | 774 | 774 |

Total: 24427 linhas, igual a Σ size(items) sobre os válidos. 0 itens órfãos.

## Observação (números seed-42 são referência, não alvo rígido)

As contagens por `event_name` em `validos_resumo.csv` são menores que as da Bronze
porque os eventos rejeitados foram removidos (ex.: `scroll` 20259 na Bronze →
19437 válidos; a diferença 822 inclui duplicatas de `scroll` e rejeições por
marco/timestamp). O que o enunciado exige — dedup, cobertura das 6 regras R7,
sessões sem `session_hint`, stitching e itens explodidos — está comprovado pela
reconciliação, não por igualdade a um número fixo.
