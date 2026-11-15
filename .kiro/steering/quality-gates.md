# Portões de qualidade

Fontes: `docs/ENUNCIADO.md` (R1–R11, rubrica da seção 7), `pipeline/README.md`,
`pyproject.toml`, testes em `tests/`.

Estes critérios decidem se uma etapa pode ser declarada concluída. Nenhuma etapa
é "validada" sem evidência de execução real.

## Padrões de código

- Reutilizar `pipeline/comum.py`; não reimplementar SparkSession, caminhos ou
  acesso ao S3.
- Acesso ao S3 exclusivamente por Spark e `s3a://`. **Sem `boto3`** no pipeline;
  sem download para disco (exceção: `site/mapa_calor.js` no R11).
- Schema explícito na Bronze (sem inferência). Parquet em todas as camadas.
- No modo ANSI do Spark 4, usar `try_divide` / `try_cast` / `try_to_timestamp`
  onde o `null` é o comportamento desejado (rejeições na Silver, taxas com
  denominador zero na Gold). Não mascarar erros reais com `try_`.
- Idioma português, seguindo o estilo do repositório.
- Não alterar componentes prontos sem necessidade demonstrada: `comum.py`,
  gerador, explorador, site, Dockerfile, docker-compose, testes existentes.

## Testes necessários

- Rodar a suíte no contêiner: `docker compose run --rm app python -m pytest`
  (coleta `tests/` e `explorador/tests/`, conforme `pyproject.toml`).
- Testes novos dos jobs do pipeline vão em `tests/pipeline/`, usando a fixture
  `spark` de `tests/pipeline/conftest.py`.
- Os testes do gerador, do site e do explorador precisam continuar passando (não
  quebrar o que já existe).
- **Não adicionar testes nesta fase de preparação.** Testes dos jobs são
  escritos junto com cada job, nas fases de implementação.

## Validação dos requisitos do enunciado

Mapa de requisito → etapa → como comprovar:

| Req | Etapa | Como se comprova |
| --- | --- | --- |
| R1 | todas | acesso ao S3 só por `spark.read`/`df.write` em `s3a://`; sem `boto3` no pipeline |
| R2 | todas | 8 jobs separados; cada um chama `exigir_camada` da dependência |
| R3 | integração | script único reconstrói o datalake a partir da landing; não gera dados |
| R4 | Bronze | append-only; schema explícito; partições `event_name/year/month/day`; colunas de proveniência (arquivo de origem + instante de ingestão) |
| R5 | Silver | cada `event_id` aparece uma vez; contagem de duplicatas descartadas |
| R6 | Silver | `params` e `element_id` viram colunas tipadas |
| R7 | Silver | `silver/eventos_rejeitados/` com um motivo por linha; cobertura das 6 regras mínimas |
| R8 | Silver | sessões por corte de 30 min por `user_pseudo_id`; **sem** `session_hint` |
| R9 | Silver | identity stitching aplica `user_id` a todos os eventos do `user_pseudo_id`, inclusive anteriores ao login |
| R10 | Silver | `silver/itens/` com `items[]` explodido, uma linha por item por evento |
| R11 | Gold + export | `mapa_calor_cliques` e `mapa_calor_rolagem` com **nomes de coluna exatos**; `site/mapa_calor.js` gerado; mapa acende no site |
| 5.3 | Gold | as 15 tabelas existem e batem com o esperado dentro da tolerância; modelo de propensão sem vazamento (features só antes do 1º `add_to_cart` da sessão) |
| 5.4 | Gold | usar as definições exatas de receita, canal, plano, funil, página, datas (UTC), mediana por `percentile_approx` |

Rubrica (seção 7): Arquitetura 15, Silver 25, Gold 30, Relatório 20, Qualidade do
dado 10. Usar como checklist final.

## Critérios para NÃO declarar uma etapa concluída

Uma etapa permanece pendente/em desenvolvimento se qualquer item abaixo falhar:

- o(s) job(s) da etapa não rodaram com sucesso no contêiner `app`, a partir de
  dados reais;
- as contagens/valores não foram conferidos contra o esperado do enunciado;
- não há evidência capturada em `entregas/<etapa>/` (log real, consulta, CSV);
- os testes relevantes não passam;
- há uso proibido de `boto3`/SDK ou download para disco no pipeline;
- as regras do enunciado foram alteradas sem registro de justificativa.

## Proibições sobre evidências

- **Nunca** produzir prints fictícios, CSVs artificiais ou números que aparentem
  evidência de execução sem que o pipeline tenha rodado de verdade.
- Não declarar que "o ambiente funciona" sem executá-lo e confirmar o resultado
  (ex.: as três linhas `[ok]` de `infra/verificar_ambiente.py`).
- Toda afirmação de resultado no `RELATORIO.md` deve ser rastreável a uma consulta
  e a um CSV real em `entregas/.../resultados/`.
