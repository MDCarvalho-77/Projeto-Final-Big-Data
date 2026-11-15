-- Validação Gold: metricas_modelo (5.3 #12).
-- EXATAMENTE 4 linhas: auc, f1, precisao, recall (classe positiva no conjunto de
-- teste). A coluna features lista as variáveis de entrada (todas pré-carrinho,
-- sem vazamento).
-- Resultado real conferido em ../resultados/metricas_modelo.csv
SELECT metrica, valor, features
FROM gold.metricas_modelo
ORDER BY metrica;
