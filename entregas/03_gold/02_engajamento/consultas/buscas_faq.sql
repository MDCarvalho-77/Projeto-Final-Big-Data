-- Validação Gold: buscas_faq (5.3 #5).
-- Por search_term: buscas = total; buscas_sem_resultado = search_results = 0.
-- Invariante: buscas_sem_resultado <= buscas. Termos sem resultado revelam
-- lacunas de conteúdo no FAQ.
-- Resultado real conferido em ../resultados/buscas_faq.csv
SELECT search_term, buscas, buscas_sem_resultado
FROM gold.buscas_faq
ORDER BY buscas_sem_resultado DESC, buscas DESC;
