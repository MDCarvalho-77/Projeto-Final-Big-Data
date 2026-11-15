-- Validação da Silver: sessionização por corte de 30 min (R8), sem session_hint.
-- Executar no explorador: Athena · Editor de consultas (banco silver).
-- Distribuição de usuários por número de sessões reconstruídas: mostra que há
-- user_pseudo_id com mais de uma sessão (cortes de inatividade > 30 min),
-- confirmando que a sessão é calculada e não copiada de session_hint.
WITH por_usuario AS (
  SELECT user_pseudo_id, count(DISTINCT session_id) AS sessoes
  FROM silver.eventos
  GROUP BY user_pseudo_id
)
SELECT sessoes, count(*) AS usuarios
FROM por_usuario
GROUP BY sessoes
ORDER BY sessoes;
