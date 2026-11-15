# Gold 02 — Engajamento

## Objetivo e escopo

Tabelas de engajamento por sessão. Requisitos: **R2**, seção 5.3 (`sessoes`,
`retencao_video`, `buscas_faq`, `saidas_downloads`).

Tabelas:
- `sessoes` — uma linha por sessão: duração, eventos, scroll máximo, maior
  `video_percent`, fez lead, comprou, canal, dispositivo.
- `retencao_video` — das sessões com `video_start`, percentual que atingiu 10, 25,
  50, 75 e 100%.
- `buscas_faq` — termo, total de buscas, buscas sem resultado.
- `saidas_downloads` — cliques de saída por domínio; downloads por arquivo.

## Arquivos oficiais de código relacionados

- `pipeline/gold_engajamento.py` (a implementar) · `pipeline/comum.py` (reutilizar).
- Fonte: `docs/ENUNCIADO.md` seção 5.3; `docs/CONTRATO_EVENTO.md` (eventos de
  vídeo, `site_search`, `click` de saída, `file_download`).

## Evidências obrigatórias

- Log real do job no contêiner `app`.
- Amostra das quatro tabelas em `gold/`.

## Consultas e resultados a registrar

- SQL conferindo cada tabela; CSVs reais em `resultados/`.

## Critérios de conclusão

- As quatro tabelas existem e batem com o esperado; `exigir_camada` da Silver
  presente.

## Estado atual

**Pendente.**
