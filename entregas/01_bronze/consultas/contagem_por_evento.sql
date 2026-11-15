-- Validação da Bronze: linhas por event_name.
-- Executar no explorador: Athena · Editor de consultas (banco bronze).
-- Confere que a ingestão preservou todos os nomes de evento da landing,
-- inclusive os fora do catálogo (ex.: add_to_wishlist) e a partição
-- __HIVE_DEFAULT_PARTITION__ (eventos sem event_name), já que a Bronze é
-- cópia fiel (a rejeição é da Silver).
SELECT event_name, count(*) AS linhas
FROM bronze.eventos
GROUP BY event_name
ORDER BY linhas DESC;
