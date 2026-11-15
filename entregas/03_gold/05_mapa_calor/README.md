# Gold 05 — Mapa de calor

## Objetivo e escopo

Tabelas do mapa de calor e exportação para o site. Requisitos: **R2, R11**, seções
5.3 e 5.5. Nomes de coluna **exatos** são obrigatórios (o site lê por esses nomes).

Tabelas:
- `mapa_calor_cliques` — uma linha por `pagina` e `element_id` (só eventos com
  `element_id` e `page_location`): colunas `cliques`, `sessoes`,
  `sessoes_na_pagina`, `taxa_clique` (= `sessoes ÷ sessoes_na_pagina`, nulo se
  divisor 0), `intensidade` (= `cliques ÷` maior `cliques` da mesma página, 0 a 1).
  Taxas a 4 casas.
- `mapa_calor_rolagem` — uma linha por marco (0, 25, 50, 75, 90) da landing
  (`pagina = "/"`): `sessoes_que_chegaram` e `pct_sessoes` (= ÷ marco 0), a 4
  casas.

## Exportação (R11)

`site/mapa_calor.js` com uma linha
`window.MAPA_CALOR = {"gerado_em": ..., "cliques": [...], "rolagem": [...]};`.
O job lê a Gold **pelo Spark** (R1) e só grava o arquivo local no fim, via
`arquivo_do_projeto("site/mapa_calor.js")`. O arquivo é gerado e **não entra no
git** (ver `.gitignore`).

## Arquivos oficiais de código relacionados

- `pipeline/gold_mapa_calor.py` e `pipeline/exportar_mapa_calor.py` (a implementar)
  · `pipeline/comum.py` (reutilizar).
- Fonte: `docs/ENUNCIADO.md` seções 5.3, 5.5 (R11); `pipeline/README.md`
  ("Exportar um resultado pequeno para o site").

## Evidências obrigatórias

- Log real dos dois jobs no contêiner `app`.
- Amostra das duas tabelas com os nomes de coluna exatos.
- Confirmação visual do mapa de calor aceso em `site/index.html` (botão
  **Mapa de calor**).

## Consultas e resultados a registrar

- SQL conferindo cliques/rolagem por página e marco; CSVs reais em `resultados/`.

## Critérios de conclusão

- As duas tabelas existem com os nomes de coluna exatos; `site/mapa_calor.js`
  gerado; mapa acende no site; `exigir_camada` da Silver/Gold presente.

## Estado atual

**Pendente.**
