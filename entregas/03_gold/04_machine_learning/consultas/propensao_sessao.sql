-- Validação Gold: propensao_sessao (5.3 #11).
-- Todas as sessões escoradas; probabilidade_compra em [0,1]; rotulo 0/1;
-- conjunto em {treino, teste}. Amostra em ../resultados/propensao_sessao_amostra.csv
SELECT
  count(*)                                  AS n_sessoes,
  min(probabilidade_compra)                 AS prob_min,
  max(probabilidade_compra)                 AS prob_max,
  sum(CASE WHEN conjunto = 'teste'  THEN 1 ELSE 0 END) AS n_teste,
  sum(CASE WHEN conjunto = 'treino' THEN 1 ELSE 0 END) AS n_treino
FROM gold.propensao_sessao;
