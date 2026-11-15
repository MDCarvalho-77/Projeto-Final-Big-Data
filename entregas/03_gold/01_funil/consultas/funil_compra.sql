-- Validação Gold: funil_compra (5.3 #1).
-- Executar no explorador: Athena · Editor de consultas (banco gold) ou DuckDB
-- sobre gold/funil_compra. Confere a monotonia do funil no escopo total e que
-- taxa_view_item é única por escopo (mesmo recorte da linha).
-- Resultado real conferido em ../resultados/funil_compra.csv
SELECT
  escopo, plano, canal, etapa, ordem_etapa, sessoes,
  conv_etapa_anterior, conv_topo, taxa_view_item
FROM gold.funil_compra
WHERE escopo = 'total'
ORDER BY ordem_etapa;
