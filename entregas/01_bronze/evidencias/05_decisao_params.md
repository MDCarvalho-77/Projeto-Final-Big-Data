# Decisão técnica: representação de `params` na Bronze

Data: 25/10/2026. Ambiente real: PySpark 4.2.0 + Parquet (floci/s3a) + DuckDB
1.5.6 (motor do explorador). Todas as afirmações abaixo vêm de execução real.

## Problema

O contrato (`docs/CONTRATO_EVENTO.md`) diz que `params` varia por evento e traz
valores de tipos JSON diferentes: texto, número (inteiro e decimal), booleano
(`outbound`) e nulo (`coupon: null`, `transaction_id: null` em eventos com
defeito). A versão anterior lia `params` como `map<string,string>`, que converte
todo valor para texto. A dúvida: isso perde informação que a Bronze (cópia fiel)
e a futura Silver precisam?

## O que foi testado (3 representações)

Dados de teste cobrindo número decimal/inteiro, booleano, texto, nulo, chave
ausente e `params` aninhado inesperado (objeto/array dentro de `params`).

| Repr. | Spark lê/grava/relê | DuckDB (explorador) lê | Preserva tipo JSON | Observação |
| --- | --- | --- | --- | --- |
| A) `map<string,string>` | OK | OK (`MAP(VARCHAR,VARCHAR)`) | não (tudo vira texto) | chaves, nulos e aninhado-como-texto preservados; perde a etiqueta de tipo |
| B) `variant` (`parse_json`) | OK | OK (lê `VARIANT`, tipos preservados) | sim | tipo mais novo; muda mais a camada e o catálogo |
| C) `map` + `params_json` cru | OK | OK | sim, em `params_json` | menor alteração que preserva tudo |

Constatações das execuções:

- Em A, um `params` aninhado inesperado **não é descartado**: o valor aninhado
  fica como texto JSON dentro do mapa (ex.: `inesperado -> {"a":1,"b":[1,2,3]}`).
  Nulos preservados (`coupon -> NULL`); chave ausente não quebra.
- B (`variant`) funciona de ponta a ponta, inclusive no DuckDB (surpresa boa:
  o explorador leria). Preserva número/booleano/aninhado com o tipo certo.
- C mantém o mapa consultável de A **e** acrescenta `params_json`, o objeto JSON
  preservando tipos/aninhamento/nulos. `params_json` é derivado numa única
  leitura (o schema lê `params` como `string`), e o mapa vem de
  `from_json(params_json, 'map<string,string>')`.

## Decisão: opção C (`map` + `params_json`)

Motivos:

1. **Fidelidade (R4):** `params_json` preserva os tipos JSON, o aninhamento e os
   nulos — nada é perdido nem inventado. É a cópia fiel que a Bronze exige.
2. **Consultabilidade:** `params` (map) continua fácil de consultar no explorador
   e serve de base direta para a tipagem por chave da Silver (R6).
3. **Menor alteração e menor risco:** uma coluna de texto a mais, sem depender da
   semântica (ainda nova) do `variant` no Data Catalog e nas views do explorador.
   O `variant` foi aprovado nos testes, mas muda mais a camada para um ganho que a
   dupla `params_json` + `map` já entrega.

### Limite técnico registrado

O Spark **reserializa** o objeto ao ler `params` como string, então `params_json`
pode ter espaçamento diferente do arquivo da landing (ex.: `{"a": 1}` em vez de
`{"a":1}`). O **conteúdo e os tipos** são fiéis; só o espaçamento pode normalizar.
Por isso os testes comparam a estrutura (`json.loads`), não o texto literal.

### O que a Bronze NÃO faz

Nenhuma tipagem por chave, validação ou rejeição — isso é da Silver (R6/R7). A
Bronze só preserva `params` de forma fiel e consultável.

## Evidência de execução

- Esquema final e as duas colunas: `02_schema_proveniencia_particoes.txt`.
- Consulta de fidelidade (compra com `coupon:null` e outra com
  `transaction_id:null` preservados): `../resultados/params_fidelidade.csv` e
  `../consultas/params_fidelidade.sql`.
- DESCRIBE pelo Athena do explorador mostrando `params_json` (VARCHAR) e `params`
  (MAP(VARCHAR,VARCHAR)): `06_catalogo_athena.txt`.
