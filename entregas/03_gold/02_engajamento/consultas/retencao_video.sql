-- Validação Gold: retencao_video (5.3 #4).
-- Marcos {10,25,50,75,100}; denominador = sessões com video_start; pct_retencao
-- decrescente (quanto maior o marco, menos sessões o atingem).
-- Resultado real conferido em ../resultados/retencao_video.csv
SELECT marco, sessoes_no_marco, pct_retencao
FROM gold.retencao_video
ORDER BY marco;
