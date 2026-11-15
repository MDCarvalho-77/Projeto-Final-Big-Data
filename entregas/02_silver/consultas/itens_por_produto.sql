-- Validação da Silver: tabela de itens explodida (R10).
-- Executar no explorador: Athena · Editor de consultas (banco silver).
-- Uma linha por item_id do catálogo com a contagem de linhas na tabela de itens
-- (items[] explodido, uma linha por item por evento), a soma de quantidade e o
-- preço. Prova que cada evento de e-commerce gerou uma linha por item.
SELECT
  item_id,
  item_name,
  item_category,
  price,
  count(*)        AS linhas_item,
  sum(quantity)   AS soma_quantidade
FROM silver.itens
GROUP BY item_id, item_name, item_category, price
ORDER BY linhas_item DESC;
