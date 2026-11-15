# Gold 01 — Funil

## Objetivo e escopo

Tabelas do funil de compra e do funil de lead. Requisitos: **R2**, seção 5.3
(linhas `funil_compra`, `funil_lead`) e definições 5.4 (canal, plano, funil).

Tabelas:
- `funil_compra` — sessões em cada etapa `view_item_list → select_item →
  add_to_cart → view_cart → begin_checkout → add_payment_info → purchase`,
  conversão sobre etapa anterior e sobre o topo; total e por plano × canal.
  `view_item` entra como taxa à parte, não na sequência.
- `funil_lead` — `page_view` da landing → `scroll` ≥ 50% → `generate_lead`, por
  canal.

## Arquivos oficiais de código relacionados

- `pipeline/gold_funil.py` (a implementar) · `pipeline/comum.py` (reutilizar).
- Fonte: `docs/ENUNCIADO.md` seções 5.3 e 5.4 (regras 3 e 4).

## Evidências obrigatórias

- Log real do job no contêiner `app`.
- Amostra de `gold/funil_compra/` e `gold/funil_lead/`.

## Consultas e resultados a registrar

- SQL conferindo contagens por etapa e conversões; CSVs reais em `resultados/`.

## Critérios de conclusão

- Ambas as tabelas existem e batem com o esperado; definições de canal/plano/funil
  aplicadas; `exigir_camada` da Silver presente.

## Estado atual

**Pendente.**
