# Entrega 03 — Gold

## Objetivo e escopo

Produzir as **15 tabelas Gold** da seção 5.3 do enunciado, em cinco jobs, mais a
exportação do mapa de calor para o site (R11). Requisitos: **R2, R11** e seções
5.3, 5.4, 5.5.

Esta entrega é subdividida nos cinco jobs Gold, cada um com sua própria pasta e
seus próprios `evidencias/`, `consultas/` e `resultados/`:

| Subpasta | Job oficial | Tabelas |
| --- | --- | --- |
| `01_funil/` | `pipeline/gold_funil.py` | `funil_compra`, `funil_lead` |
| `02_engajamento/` | `pipeline/gold_engajamento.py` | `sessoes`, `retencao_video`, `buscas_faq`, `saidas_downloads` |
| `03_receita_aquisicao/` | `pipeline/gold_receita_aquisicao.py` | `receita_diaria`, `taxa_reembolso`, `adesao_order_bump`, `lead_para_compra` |
| `04_machine_learning/` | `pipeline/gold_ml.py` | `propensao_sessao`, `metricas_modelo`, `segmentos_visitantes` |
| `05_mapa_calor/` | `pipeline/gold_mapa_calor.py` + `pipeline/exportar_mapa_calor.py` | `mapa_calor_cliques`, `mapa_calor_rolagem`, `site/mapa_calor.js` |

(Os nomes de arquivo são a convenção proposta em `.kiro/steering/structure.md`.)

## Arquivos oficiais de código relacionados

- Os jobs Gold em `pipeline/` listados acima (a implementar).
- `pipeline/comum.py` (reutilizar), com `exigir_camada` da Silver em cada job.
- Fontes de requisito: `docs/ENUNCIADO.md` seções 5.3 (conteúdo das 15 tabelas),
  5.4 (definições exatas) e 5.5 (R11).

## Evidências obrigatórias (por subpasta)

- Log real de cada job Gold no contêiner `app`.
- Amostra de cada tabela produzida em `gold/<tabela>/`.
- Para o ML: comprovação de que as features usam só comportamento **antes do
  primeiro `add_to_cart`** da sessão (sem vazamento), e as métricas AUC/F1
  (recomendado: precisão/recall) do conjunto de teste.
- Para o mapa de calor: nomes de coluna **exatos** e o mapa acendendo no site.

## Consultas e resultados a registrar

- Uma consulta por tabela conferindo os números-chave contra o esperado.
- CSVs reais em `resultados/` de cada subpasta.

## Critérios de conclusão

- As 15 tabelas existem e batem com o esperado dentro da tolerância.
- Definições da seção 5.4 aplicadas (receita, canal, plano, funil, página, datas
  UTC, mediana via `percentile_approx`).
- Modelo de propensão sem vazamento de informação.
- `mapa_calor_cliques` e `mapa_calor_rolagem` com nomes de coluna exatos;
  `site/mapa_calor.js` gerado e mapa aceso no site (R11).
- Testes relevantes passando; evidências capturadas.

## Estado atual

**Validada.** Os cinco jobs Gold, o exportador e o orquestrador estão
implementados em `pipeline/`, com testes passando (suíte completa: 436 passed). A
execução integrada real (`pipeline/executar_pipeline.py`, rc=0, seed 42) gravou as
15 tabelas em `gold/` e `site/mapa_calor.js`.

Contagens reais conferidas (seed 42):

| Tabela | Linhas | Tabela | Linhas |
| --- | ---: | --- | ---: |
| funil_compra | 147 | receita_diaria | 184 |
| funil_lead | 6 | taxa_reembolso | 15 |
| sessoes | 8.793 | adesao_order_bump | 1 |
| retencao_video | 5 | lead_para_compra | 6 |
| buscas_faq | 10 | propensao_sessao | 8.793 |
| saidas_downloads | 5 | metricas_modelo | 4 |
| mapa_calor_cliques | 24 | segmentos_visitantes | 5.994 |
| mapa_calor_rolagem | 5 | segmentos_perfil | 5 |

Invariantes conferidas (`entregas/04_integracao_final/evidencias/validacao_gold.txt`):
funil monotônico (5258 ≥ 1719 ≥ 1291 ≥ 1255 ≥ 912 ≥ 684 ≥ 573); `taxa_view_item`
única por escopo; `receita_bruta ≥ receita_reembolsada` por linha (0 violações);
`metricas_modelo` com 4 linhas (auc=0,9227, f1=0,4773, precisao=0,3172,
recall=0,9633); `intensidade` ∈ [0,1] e máx. por página = 1; `pct_sessoes`
monotônico. Cada tabela tem `.sql` em `consultas/` e CSV real em `resultados/`.
