-- Validação da Silver: eventos rejeitados por motivo (R7).
-- Executar no explorador: Athena · Editor de consultas (banco silver).
-- Uma linha por motivo (um motivo por linha na tabela de rejeitados), com a
-- contagem de linhas e de eventos distintos. Cobre as 6 regras mínimas do R7.
SELECT
  motivo_rejeicao,
  count(*)                   AS linhas,
  count(DISTINCT event_id)   AS eventos_distintos
FROM silver.eventos_rejeitados
GROUP BY motivo_rejeicao
ORDER BY linhas DESC;
