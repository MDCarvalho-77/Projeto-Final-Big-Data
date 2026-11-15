-- Validação da Bronze: proveniência e partições.
-- Executar no explorador: Athena · Editor de consultas (banco bronze).

-- 1) Proveniência (R4): toda linha tem arquivo de origem e instante de ingestão.
SELECT
  count(*)                                   AS total,
  count("_arquivo_origem")                   AS com_arquivo_origem,
  count("_ingerido_em")                      AS com_ingerido_em,
  count(DISTINCT "_arquivo_origem")          AS arquivos_distintos
FROM bronze.eventos;

-- 2) Partições (R4): contagem por event_name/year/month/day, com month/day de
--    dois dígitos (texto). Mostra as 20 primeiras partições.
SELECT event_name, year, month, day, count(*) AS linhas
FROM bronze.eventos
GROUP BY event_name, year, month, day
ORDER BY year, month, day, event_name
LIMIT 20;
