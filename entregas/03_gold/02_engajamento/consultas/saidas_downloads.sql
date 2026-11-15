-- Validação Gold: saidas_downloads (5.3 #6).
-- tipo='saida' = click outbound agrupado por link_domain (domínio nulo descartado
-- da visão de saída, documentado no RELATORIO); tipo='download' = file_download
-- por file_name (nunca descartado por link_domain nulo).
-- Resultado real conferido em ../resultados/saidas_downloads.csv
SELECT tipo, chave, ocorrencias, sessoes
FROM gold.saidas_downloads
ORDER BY tipo, ocorrencias DESC;
