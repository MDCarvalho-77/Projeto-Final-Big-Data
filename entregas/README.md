# entregas/ — pacotes de entrega parcial

Esta pasta organiza as **entregas incrementais** do projeto DadosPro, uma por
camada da arquitetura medalhão, mais a integração final.

## Propósito

Reunir, em um lugar previsível, as **evidências de execução**, as **consultas
SQL** e os **resultados reais** de cada etapa do pipeline. Serve para acompanhar o
progresso e montar os pacotes de entrega para avaliação.

Esta pasta **não contém o código-fonte** dos jobs. O código canônico vive em
`pipeline/` e os testes em `tests/`. Os READMEs de cada etapa apenas referenciam
esses arquivos oficiais pelo caminho.

## Estrutura

```text
entregas/
├── README.md                 (este arquivo)
├── 01_bronze/                camada Bronze (R1, R2, R4)
├── 02_silver/                camada Silver (R5–R10)
├── 03_gold/                  camada Gold (seção 5.3), 5 subpastas:
│   ├── 01_funil/
│   ├── 02_engajamento/
│   ├── 03_receita_aquisicao/
│   ├── 04_machine_learning/
│   └── 05_mapa_calor/        inclui a exportação do R11
└── 04_integracao_final/      script de sequência (R3) + relatório
```

Cada etapa (e cada subpasta Gold) tem três pastas padrão:

- `evidencias/` — saídas de console reais capturadas do contêiner `app` ou do
  explorador (logs de jobs, contagens, prints das telas S3/Glue/Athena).
- `consultas/` — consultas SQL (`.sql`) usadas no editor Athena do explorador para
  conferir cada tabela.
- `resultados/` — os CSVs reais exportados dessas consultas e o resumo dos números
  que entram no `RELATORIO.md`.

## Conteúdo esperado por entrega

Cada `README.md` de etapa descreve: objetivo e escopo, arquivos oficiais de código
relacionados, evidências obrigatórias, consultas e resultados a registrar,
critérios de conclusão e o estado atual da etapa.

## Regras

- Não duplicar o código dos jobs aqui. Referenciar `pipeline/` e `tests/`.
- Registrar apenas resultados produzidos por execução real do pipeline. Proibido
  fabricar prints, CSVs ou números.
- Cada resultado deve ser rastreável à consulta que o gerou.
- O relatório final `RELATORIO.md` fica na **raiz do projeto**, não aqui.

## Estado geral

Todas as etapas estão **pendentes**. Nenhuma foi implementada ou validada ainda.
A implementação começa pela Bronze, em tarefa posterior.
