-- Validação Gold: adesao_order_bump (5.3 #9).
-- Visão global: compras = purchase distintos por event_id; compras_com_bump =
-- purchase cujo event_id aparece em silver/itens com item_id='BUMP-MENTORIA'
-- (join purchase->itens por event_id); taxa_adesao = try_divide.
-- Resultado real conferido em ../resultados/adesao_order_bump.csv
SELECT compras, compras_com_bump, taxa_adesao
FROM gold.adesao_order_bump;
