-- Validação da Silver: eventos válidos (R5, R6, R8, R9).
-- Executar no explorador: Athena · Editor de consultas (banco silver).
-- Uma linha por event_name com: total de eventos válidos, event_id distintos
-- (prova da dedup R5: total == distintos), sessões distintas (R8) e quantos
-- eventos receberam user_id por stitching (R9: user_id original nulo, resolvido
-- preenchido).
SELECT
  event_name,
  count(*)                                                         AS validos,
  count(DISTINCT event_id)                                         AS event_id_distintos,
  count(DISTINCT session_id)                                       AS sessoes,
  count(*) FILTER (WHERE user_id IS NULL AND user_id_resolvido IS NOT NULL) AS stitching_aplicado
FROM silver.eventos
GROUP BY event_name
ORDER BY validos DESC;
