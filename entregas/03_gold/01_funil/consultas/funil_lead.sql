-- Validação Gold: funil_lead (5.3 #2).
-- Invariante conferida: sessoes_lead <= sessoes_scroll_50 <= sessoes_page_view
-- por canal e no sentinela '(total)'. conv_scroll/conv_lead via try_divide.
-- Resultado real conferido em ../resultados/funil_lead.csv
SELECT
  canal, sessoes_page_view, sessoes_scroll_50, sessoes_lead,
  conv_scroll, conv_lead
FROM gold.funil_lead
ORDER BY canal;
