-- Validação da Bronze: fidelidade do campo params.
-- Executar no explorador: Athena · Editor de consultas (banco bronze).
-- A Bronze guarda params de duas formas: params_json (objeto JSON, fiel aos
-- tipos) e params (map<string,string> consultável). Esta consulta mostra as duas
-- numa compra, provando que os tipos (number/boolean/null) ficam em params_json.
SELECT
  event_id,
  params_json,
  params['transaction_id'] AS transaction_id_do_map,
  params['value']          AS value_do_map
FROM bronze.eventos
WHERE event_name = 'purchase'
  AND params_json LIKE '%transaction_id%'
LIMIT 5;
