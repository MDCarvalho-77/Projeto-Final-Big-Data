-- Validação Gold: mapa_calor_rolagem (5.3 #15, R11 — NOMES DE COLUNA EXATOS).
-- Colunas exatas: pagina, marco, sessoes_que_chegaram, pct_sessoes.
-- Marcos {0,25,50,75,90} na landing ('/'); marco=0 => pct_sessoes=1.0;
-- pct_sessoes monotônico decrescente.
-- Resultado real conferido em ../resultados/mapa_calor_rolagem.csv
SELECT pagina, marco, sessoes_que_chegaram, pct_sessoes
FROM gold.mapa_calor_rolagem
ORDER BY pagina, marco;
