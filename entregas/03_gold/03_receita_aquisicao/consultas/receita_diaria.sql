-- Validação Gold: receita_diaria (5.3 #7).
-- Granularidade (data_compra, plano, canal). receita_liquida = bruta - reembolsada.
-- Invariante por linha: receita_bruta >= receita_reembolsada (LEFT JOIN
-- purchase ⟕ refund; reembolso lançado na data da compra). Totais agregados:
-- Resultado real conferido em ../resultados/receita_diaria.csv
SELECT
  round(sum(receita_bruta), 2)       AS receita_bruta,
  round(sum(receita_reembolsada), 2) AS receita_reembolsada,
  round(sum(receita_liquida), 2)     AS receita_liquida,
  sum(pedidos)                       AS pedidos,
  sum(CASE WHEN receita_reembolsada > receita_bruta THEN 1 ELSE 0 END) AS violacoes
FROM gold.receita_diaria;
