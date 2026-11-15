-- Validação Gold: taxa_reembolso (5.3 #8).
-- Por (plano, canal): pedidos_com_reembolso = compras distintas por transaction_id
-- com >= 1 refund; taxa_reembolso = try_divide(pedidos_com_reembolso, pedidos).
-- Resultado real conferido em ../resultados/taxa_reembolso.csv
SELECT plano, canal, pedidos, pedidos_com_reembolso, taxa_reembolso
FROM gold.taxa_reembolso
ORDER BY taxa_reembolso DESC;
