-- Validação Gold: mapa_calor_cliques (5.3 #14, R11 — NOMES DE COLUNA EXATOS).
-- Colunas exatas: pagina, element_id, cliques, sessoes, sessoes_na_pagina,
-- taxa_clique, intensidade. Invariantes: intensidade em [0,1] e max por pagina = 1.
-- Resultado real conferido em ../resultados/mapa_calor_cliques.csv
SELECT pagina, element_id, cliques, sessoes, sessoes_na_pagina,
       taxa_clique, intensidade
FROM gold.mapa_calor_cliques
ORDER BY pagina, cliques DESC;
