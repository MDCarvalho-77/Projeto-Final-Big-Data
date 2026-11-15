# Processo de entregas incrementais

Fontes: `docs/ENUNCIADO.md` (R1–R11, seções 5 e 7), `docs/CONTRATO_EVENTO.md`.

O desenvolvimento é por camada, uma de cada vez, só avançando quando a anterior
está funcionando e com evidências. As evidências de cada etapa ficam em
`entregas/`; o código canônico permanece em `pipeline/` e os testes em `tests/`.

## Estratégia por camada

A pasta `entregas/` organiza as entregas parciais. Ela **não** contém código dos
jobs — apenas READMEs que apontam para os arquivos oficiais, evidências de
execução, consultas SQL e resultados (CSVs reais).

```text
entregas/
├── README.md
├── 01_bronze/          evidencias/ consultas/ resultados/
├── 02_silver/          evidencias/ consultas/ resultados/
├── 03_gold/            01_funil/ 02_engajamento/ 03_receita_aquisicao/
│                       04_machine_learning/ 05_mapa_calor/
│                       (cada um com evidencias/ consultas/ resultados/)
└── 04_integracao_final/ evidencias/ consultas/ resultados/
```

### Ordem de desenvolvimento (ENUNCIADO "Por onde começar")

0. Validar ambiente: `floci` de pé, gerador rodado, `verificar_ambiente.py` com
   três linhas `[ok]`.
1. **Bronze** (R1, R2, R4) → entrega `01_bronze`.
2. **Silver** (R5–R10) → entrega `02_silver`.
3. **Gold**, uma tabela por vez (seção 5.3) → entrega `03_gold`, subdividida nos
   cinco jobs.
4. **Mapa de calor no site** (R11) → parte de `03_gold/05_mapa_calor`.
5. **Script de sequência** dos 8 jobs (R3) + relatório → `04_integracao_final`.

## Critérios de conclusão por etapa

Uma etapa só é marcada **validada** quando:

- os jobs da etapa rodam no contêiner `app` sem erro, a partir da landing/camada
  anterior;
- os testes relevantes em `tests/` passam;
- as contagens/valores estão dentro da tolerância esperada pelo enunciado (quando
  aplicável);
- as evidências estão registradas em `entregas/<etapa>/` (ver abaixo).

Enquanto faltar qualquer item, a etapa fica **pendente** ou **em desenvolvimento**.
Nunca marcar como validada sem evidência de execução real.

## Localização de evidências, consultas e resultados

Para cada etapa em `entregas/<etapa>/`:

- `evidencias/` — saídas de console reais capturadas do contêiner `app` ou do
  explorador (logs dos jobs, contagens, prints da tela S3/Glue/Athena). Arquivos
  de texto (`.txt`/`.md`) ou imagens.
- `consultas/` — as consultas SQL (`.sql`) usadas no explorador (Athena) para
  conferir os números de cada tabela.
- `resultados/` — os CSVs **reais** exportados dessas consultas, mais um resumo
  dos números que entram no `RELATORIO.md`.

As tabelas Gold vivem no S3 (`gold/<tabela>/` em Parquet). Em `entregas/` ficam só
os extratos conferidos, não cópias das tabelas inteiras.

## Regras para gerar os pacotes de entrega (fase posterior)

- Não duplicar código: os READMEs de etapa referenciam os arquivos de `pipeline/`
  e `tests/` pelo caminho, nunca copiam os jobs para dentro de `entregas/`.
- Só registrar resultados produzidos por execução real do pipeline. **Proibido**
  fabricar prints, CSVs ou números que simulem evidência.
- Cada resultado deve ser rastreável até a consulta que o gerou (guardar a query
  em `consultas/` ao lado do CSV em `resultados/`).
- O relatório final `RELATORIO.md` (raiz do projeto) cita as tabelas Gold e os
  números das pastas `resultados/`.
- Atualizar o `Estado atual` no README de cada etapa conforme ela avança:
  `pendente` → `em desenvolvimento` → `validada`.
