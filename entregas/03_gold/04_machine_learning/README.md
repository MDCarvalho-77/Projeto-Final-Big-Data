# Gold 04 — Machine learning

## Objetivo e escopo

Modelo de propensão por sessão e segmentação de visitantes. Requisitos: **R2**,
seção 5.3 e definições 5.4 (regras 6, 11) e a restrição de vazamento.

Tabelas:
- `propensao_sessao` — probabilidade de compra por sessão (modelo de
  classificação); uma linha por sessão da Silver; rótulo = sessão contém
  `purchase`.
- `metricas_modelo` — AUC e F1 no conjunto de teste e a lista de features usadas
  (recomendado: também precisão e recall da classe positiva).
- `segmentos_visitantes` — grupo de cada visitante (clusterização) e perfil médio
  de cada grupo.

## Restrição obrigatória (sem vazamento)

As features só podem usar comportamento na landing **antes do primeiro
`add_to_cart` da sessão**: rolagem, vídeo, tabela de planos vista, plano
selecionado, detalhe aberto, buscas, downloads, cliques, canal e dispositivo.
Qualquer evento do checkout (`view_cart` em diante) é proibido como feature.

## Arquivos oficiais de código relacionados

- `pipeline/gold_ml.py` (a implementar) · `pipeline/comum.py` (reutilizar).
- Fonte: `docs/ENUNCIADO.md` seção 5.3 (restrição do modelo), 5.4 (regras 6 e 11).

## Evidências obrigatórias

- Log real do job no contêiner `app`.
- Comprovação das features usadas (sem eventos de checkout) e das métricas do
  conjunto de teste.
- Amostra de `propensao_sessao`, `metricas_modelo`, `segmentos_visitantes`.

## Consultas e resultados a registrar

- SQL/evidência das métricas e dos segmentos; CSVs reais em `resultados/`.

## Critérios de conclusão

- As três tabelas existem; modelo sem vazamento; métricas registradas;
  `exigir_camada` da Silver presente.

## Estado atual

**Pendente.**
