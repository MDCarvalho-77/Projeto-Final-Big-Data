-- Validação Gold: sessoes (5.3 #3).
-- Uma linha por session_id; duracao_seg >= 0; comprou/fez_lead booleanos.
-- A contagem total deve bater com a referência da Silver (seed 42): 8793 sessões.
-- Resultado real conferido em ../resultados/sessoes.csv
SELECT count(*) AS n_sessoes,
       sum(CASE WHEN comprou THEN 1 ELSE 0 END)   AS sessoes_com_compra,
       sum(CASE WHEN fez_lead THEN 1 ELSE 0 END)  AS sessoes_com_lead,
       min(duracao_seg) AS dur_min,
       round(avg(duracao_seg), 1) AS dur_media
FROM gold.sessoes;
