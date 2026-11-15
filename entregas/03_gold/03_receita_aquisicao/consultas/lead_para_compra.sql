-- Validação Gold: lead_para_compra (5.3 #10).
-- Unidade = pessoa coalesce(user_id_resolvido, user_pseudo_id). 1º generate_lead
-- e 1ª compra estritamente depois; dias fracionário; mediana via
-- percentile_approx. Escopos '(total)' e 'por_canal'.
-- Resultado real conferido em ../resultados/lead_para_compra.csv
SELECT escopo, canal, leads, leads_que_compraram, taxa_conversao, mediana_dias
FROM gold.lead_para_compra
ORDER BY escopo, canal;
