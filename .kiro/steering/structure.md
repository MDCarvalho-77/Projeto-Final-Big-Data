# Estrutura do repositório

Confirmada por inspeção do workspace. Fontes: árvore real do repositório,
`README.md`, `pipeline/README.md`, `pyproject.toml`.

## Diretórios e responsabilidades

| Caminho | Responsabilidade | Status |
| --- | --- | --- |
| `site/` | landing page de exemplo (`index.html`) que dispara os 20 eventos, exporta JSONL e desenha o mapa de calor | pronto, não alterar |
| `gerador/` | coletor simulado: popula `landing/` com ~14 dias de eventos sintéticos, inclusive com defeitos; usa `boto3` (permitido só aqui) | pronto, não alterar |
| `pipeline/` | **código do datalake (seu trabalho)**: um job por camada | a construir |
| `pipeline/comum.py` | peças prontas (SparkSession, caminhos, leitura/gravação de partições, verificação de camada) | pronto, importar, não reescrever |
| `pipeline/README.md` | como rodar os jobs e acessar o S3; atualizar para documentar o pipeline construído | pronto, atualizar no fim |
| `explorador/` | console Streamlit (S3, Glue Catalog, Jobs ETL, Workflows, Athena SQL) | pronto, não alterar |
| `infra/verificar_ambiente.py` | confere Spark ↔ floci por `s3a://` (três linhas `[ok]`) | pronto, não alterar |
| `docs/` | `ENUNCIADO.md` (requisitos R1–R11) e `CONTRATO_EVENTO.md` (formato do evento) | fonte de verdade |
| `tests/` | testes oficiais do projeto (ver abaixo) | manter |
| `explorador/tests/` | testes do console explorador | pronto, não alterar |
| `entregas/` | pacotes de entrega parcial por camada (só evidências, consultas e resultados — **não** código) | criado nesta preparação |

## Onde ficam os jobs do pipeline (a criar)

O enunciado (R2) pede **oito jobs**, cada um num arquivo próprio em `pipeline/`:

```text
pipeline/
├── comum.py            (pronto)
├── bronze.py           job Bronze (R4)
├── silver.py           job Silver (R5–R10)
├── gold_funil.py                Gold funil (funil_compra, funil_lead)
├── gold_engajamento.py          Gold engajamento (sessoes, retencao_video, buscas_faq, saidas_downloads)
├── gold_receita_aquisicao.py    Gold receita/aquisição (receita_diaria, taxa_reembolso, adesao_order_bump, lead_para_compra)
├── gold_ml.py                   Gold ML (propensao_sessao, metricas_modelo, segmentos_visitantes)
├── gold_mapa_calor.py           Gold mapa de calor (mapa_calor_cliques, mapa_calor_rolagem)
├── exportar_mapa_calor.py       exporta site/mapa_calor.js (R11)
└── executar_pipeline.py         script de sequência dos 8 jobs (R3)
```

Os nomes de arquivo acima são a convenção proposta para este repositório; o que o
enunciado exige é um arquivo por job e a separação por camada. Ajustar os nomes
se o desenvolvimento revelar razão para isso, mantendo a regra "um job por
arquivo".

Workflows do explorador (opcional, gerados pela tela Glue · Workflows) ficam em
`pipeline/workflows/<nome>.json`.

## Onde ficam os testes

Configuração em `pyproject.toml`: `testpaths = ["tests", "explorador/tests"]`,
`pythonpath = ["."]`.

```text
tests/
├── gerador/     testes do gerador (prontos)
├── pipeline/    testes do pipeline — hoje só test_comum.py; os testes dos
│                jobs Bronze/Silver/Gold vão aqui conforme forem implementados
│                (conftest.py já fornece a fixture `spark` local)
└── site/        testes dos elementos do site (prontos)
explorador/tests/  testes do console (prontos)
```

Novos testes de jobs do pipeline vão em `tests/pipeline/`, usando a fixture
`spark` de `tests/pipeline/conftest.py` (SparkSession `local[2]`, sem UI).

## Saídas geradas (não versionadas — ver .gitignore)

`site/mapa_calor.js` (resultado do R11), `saida/`, `dados/`, `camadas/`,
`spark-warehouse/`, `*.jsonl`, `.explorador/` são artefatos de execução e **não**
entram no git. Os dados das camadas vivem no floci (S3), não no disco.

## Convenções

- Idioma: português (código, comentários, documentação e nomes de arquivos de
  apoio), seguindo o padrão do repositório.
- Caminhos de camada e partições exatamente como em
  `.kiro/steering/tech.md` e na seção 5.4 do enunciado.
- Os jobs importam de `comum`, não reimplementam acesso ao S3.
- O `RELATORIO.md` final fica na **raiz** do projeto.
- A pasta `entregas/` referencia os arquivos oficiais de `pipeline/` e `tests/`;
  nunca duplica o código-fonte dos jobs.
