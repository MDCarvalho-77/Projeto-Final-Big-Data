-- Validação Gold: segmentos_visitantes (5.3 #13) + segmentos_perfil (par que
-- satisfaz a tabela obrigatória #13). Um segmento por visitante (KMeans, K por
-- silhueta). O perfil traz as médias por segmento.
-- Resultados reais: ../resultados/segmentos_visitantes_amostra.csv e
--                   ../resultados/segmentos_perfil.csv

-- 1) 1 segmento por user_pseudo_id e distribuição:
SELECT segmento, count(*) AS n_visitantes
FROM gold.segmentos_visitantes
GROUP BY segmento
ORDER BY segmento;

-- 2) Perfil médio por segmento:
SELECT *
FROM gold.segmentos_perfil
ORDER BY segmento;
