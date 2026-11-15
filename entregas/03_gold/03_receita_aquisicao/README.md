# Gold 03 — Receita e aquisição

## Objetivo e escopo

Tabelas de receita e aquisição. Requisitos: **R2**, seção 5.3 e definições 5.4
(receita, reembolso, lead → compra, mediana).

Tabelas:
- `receita_diaria` — por data da compra (UTC), plano e canal: receita bruta,
  reembolsada e líquida, pedidos, ticket médio.
- `taxa_reembolso` — por plano e canal.
- `adesao_order_bump` — percentual de compras com `BUMP-MENTORIA`.
- `lead_para_compra` — leads, quantos compraram depois, mediana de dias entre lead
  e compra (via `percentile_approx`).

## Arquivos oficiais de código relacionados

- `pipeline/gold_receita_aquisicao.py` (a implementar) · `pipeline/comum.py`
  (reutilizar).
- Fonte: `docs/ENUNCIADO.md` seções 5.3 e 5.4 (regras 2, 5, 7, 8, 9, 10).

## Evidências obrigatórias

- Log real do job no contêiner `app`.
- Amostra das quatro tabelas em `gold/`.

## Consultas e resultados a registrar

- SQL conferindo receita bruta/líquida, ticket médio, taxa de reembolso, adesão do
  bump e mediana lead→compra; CSVs reais em `resultados/`.

## Critérios de conclusão

- As quatro tabelas existem e batem com o esperado; definições de receita e
  reembolso aplicadas (desconto Pix já no `value` de `purchase`/`refund`; reembolso
  atribuído pelo `transaction_id`); `exigir_camada` da Silver presente.

## Estado atual

**Pendente.**
