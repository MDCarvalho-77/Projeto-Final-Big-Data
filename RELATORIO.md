# Relatório final — DadosPro (Engenharia de Dados)

Datalake medalhão (Bronze → Silver → Gold) sobre os 20 eventos de tracking da
landing page do curso DadosPro. Todos os números deste relatório vêm de uma
execução real do pipeline (`pipeline/executar_pipeline.py`, rc=0) sobre a landing
gerada com `seed 42`, e são rastreáveis às consultas em
`entregas/03_gold/<sub>/consultas/*.sql` e aos CSVs reais em
`entregas/03_gold/<sub>/resultados/*.csv`. A evidência da execução dos 8 jobs
está em `entregas/04_integracao_final/evidencias/execucao_pipeline.txt`.

Referência de volume (seed 42): 64.372 linhas na landing → Bronze 64.372 (append)
→ Silver 61.813 válidos, 656 duplicatas, 1.903 rejeitados, 24.427 itens →
8.793 sessões.

---

## 1. Funil — onde a jornada de compra perde mais gente?

Fonte: `gold/funil_compra` (escopo total) —
`entregas/03_gold/01_funil/resultados/funil_compra.csv`.

| Etapa | Sessões | Conv. etapa anterior | Conv. topo |
| --- | ---: | ---: | ---: |
| view_item_list | 5.258 | — | 1,000 |
| select_item | 1.719 | 0,327 | 0,327 |
| add_to_cart | 1.291 | 0,751 | 0,246 |
| view_cart | 1.255 | 0,972 | 0,239 |
| begin_checkout | 912 | 0,727 | 0,173 |
| add_payment_info | 684 | 0,750 | 0,130 |
| purchase | 573 | 0,838 | 0,109 |

**Resposta.** A maior queda acontece logo no topo: de `view_item_list` para
`select_item` a conversão é de apenas **32,7%** — ou seja, 2 em cada 3 sessões que
veem a lista de planos não chegam a selecionar um plano. A segunda maior perda é
`begin_checkout → add_payment_info` (75%) e, dentro do checkout, a passagem
`view_cart → begin_checkout` (72,7%). A conversão do topo ao `purchase` é de
**10,9%**.

**Recomendação.** Priorizar a etapa lista → seleção de plano (teste A/B de
copy/preço/destaque de plano na seção de planos), onde está a maior fuga absoluta
(5.258 → 1.719). O checkout em si converte bem (`select_item → purchase` ≈ 33%).

O funil de leads da landing (`gold/funil_lead`,
`entregas/03_gold/01_funil/resultados/funil_lead.csv`) confirma a assimetria por
canal: `google / organic` tem a melhor conversão a lead (9,8%) e
`newsletter / email` a pior (3,8%).

---

## 2. Engajamento — o vídeo é visto até o fim? O que não acham no FAQ?

Fontes: `gold/retencao_video`, `gold/buscas_faq`, `gold/saidas_downloads`,
`gold/sessoes` — `entregas/03_gold/02_engajamento/resultados/`.

Retenção do vídeo de apresentação (VSL), denominador = sessões com `video_start`:

| Marco | Sessões | Retenção |
| ---: | ---: | ---: |
| 10% | 2.626 | 90,9% |
| 25% | 2.138 | 74,0% |
| 50% | 1.474 | 51,0% |
| 75% | 1.105 | 38,2% |
| 100% | 877 | 30,4% |

**Resposta (vídeo).** Metade das sessões que iniciam o vídeo abandona antes dos
50%, e só **30,4%** assiste até o fim. A perda mais acentuada está entre 25% e 50%.

**Resposta (FAQ).** As buscas sem resultado apontam lacunas claras de conteúdo
(`buscas_faq`): os termos mais buscados **sem nenhum resultado** são
`estagio` (44), `presencial` (33), `bolsa` (32), `power bi` (28) e `linux` (24).
Já termos como `parcelar`, `acesso`, `certificado`, `reembolso` e `apostila`
retornam resultado.

Saídas e downloads (`saidas_downloads`): o download dominante é
`ementa-formacao-dados.pdf` (335 ocorrências); as saídas externas vão
principalmente para `instagram.com` (102), `linkedin.com` (92), `wa.me` (87) e
`youtube.com` (85).

**Recomendação.** Encurtar/retrabalhar o trecho do VSL entre 25% e 50% (maior
desistência). No FAQ, criar respostas para estágio, aulas presenciais, bolsa e
ferramentas (Power BI, Linux), que hoje geram busca sem resposta.

---

## 3. Receita e aquisição — quanto entrou de fato e qual canal retém?

Fontes: `gold/receita_diaria`, `gold/taxa_reembolso`, `gold/adesao_order_bump`,
`gold/lead_para_compra` — `entregas/03_gold/03_receita_aquisicao/resultados/`.

Totais do período (`receita_diaria`):

| Receita bruta | Reembolsada | Receita líquida | Pedidos |
| ---: | ---: | ---: | ---: |
| R$ 338.523,10 | R$ 28.608,50 | **R$ 309.914,60** | 573 |

Receita líquida por canal e por plano:

| Canal | Líquida | Pedidos |
| --- | ---: | ---: |
| google / cpc | R$ 119.631,15 | 221 |
| direct / (none) | R$ 62.143,25 | 112 |
| google / organic | R$ 54.787,70 | 101 |
| newsletter / email | R$ 45.415,65 | 74 |
| instagram / paid_social | R$ 27.936,85 | 65 |

| Plano | Líquida | Pedidos |
| --- | ---: | ---: |
| PLANO-PRO | R$ 145.795,65 | 260 |
| PLANO-PREMIUM | R$ 108.812,75 | 120 |
| PLANO-BASICO | R$ 55.306,20 | 193 |

**Resposta.** A receita líquida do período foi de **R$ 309.914,60** (bruta
R$ 338.523,10 menos R$ 28.608,50 de reembolsos). O canal mais relevante em receita
é `google / cpc`. O **order bump** (mentoria) foi aderido em **90 de 573 compras
(15,7%)** — `adesao_order_bump`.

Qualidade da aquisição: o canal `instagram / paid_social` concentra as maiores
taxas de reembolso (`taxa_reembolso`), chegando a **40%** em PLANO-PREMIUM e ~19%
em PLANO-BASICO; já `direct / (none)` e `newsletter / email` têm reembolso
próximo de zero. Em lead → compra (`lead_para_compra`), a conversão total é de
**7,66%** (40 de 522 leads), com mediana de **4,6 dias** entre o lead e a compra;
por canal, `google / cpc` converte melhor (9,7%).

**Recomendação.** `instagram / paid_social` traz volume, mas com reembolso alto —
revisar segmentação/promessa do anúncio nesse canal. `google / cpc` combina maior
receita e boa conversão de lead: candidato a aumento de investimento.

---

## 4. Previsão — quais sessões estão perto de comprar e que perfis existem?

Fontes: `gold/propensao_sessao`, `gold/metricas_modelo`,
`gold/segmentos_visitantes` + `gold/segmentos_perfil` —
`entregas/03_gold/04_machine_learning/resultados/`.

Modelo de propensão à compra por sessão (regressão logística, features **somente
anteriores ao 1º `add_to_cart`** da sessão). Métricas na partição de teste
(`metricas_modelo`, exatamente 4 linhas):

| Métrica | Valor |
| --- | ---: |
| auc | 0,9227 |
| f1 | 0,4773 |
| precisao | 0,3172 |
| recall | 0,9633 |

**Resposta.** O modelo separa bem compradores de não compradores (**AUC 0,923**) e
captura quase todos os compradores (**recall 0,96**), ao custo de precisão baixa
(0,32) — esperado numa base muito desbalanceada (compra é evento raro). O escore
`probabilidade_compra` está disponível para todas as 8.793 sessões em
`propensao_sessao`.

Segmentos de visitantes (KMeans, **K=5** escolhido por silhueta = 0,59;
`segmentos_perfil`):

- **Seg. 4 (571 visitantes)** — único segmento comprador (`comprou_alguma_vez`=1),
  mais sessões e mais eventos por visitante: os clientes.
- **Seg. 3 (1.004)** — altíssimo consumo de vídeo (vídeo médio 83%), scroll alto,
  mas não compra: engajado "assistindo", ainda não convertido.
- **Seg. 2 (440)** — muitas sessões (3,3 em média) e vídeo médio 36%: pesquisador
  recorrente.
- **Seg. 0 (2.681)** — scroll alto (73%), baixo vídeo, 1 sessão: leitor da landing.
- **Seg. 1 (1.298)** — baixo engajamento em tudo (scroll 16%): rejeição rápida.

**Recomendação.** Direcionar remarketing ao Seg. 3 (muito vídeo, zero compra) com
oferta/depoimento, e usar o `probabilidade_compra` para priorizar contato com
sessões de alto escore que ainda não compraram.

**Limitações do modelo.** Base **sintética** (gerador com `seed 42`), então os
padrões refletem as regras do gerador, não o mercado real. Classe fortemente
**desbalanceada** (poucos compradores), o que puxa a precisão para baixo apesar do
AUC alto. As features são **estritamente pré-carrinho** (corte no 1º `add_to_cart`
e lista negra dos eventos de checkout) para impedir vazamento de informação
futura — o modelo não "enxerga" nada que só acontece depois da intenção de compra.

---

## 5. Qualidade do dado

Fonte: job Silver (R5–R10), evidência em
`entregas/04_integracao_final/evidencias/execucao_pipeline.txt` (etapa Silver) e
`entregas/02_silver/`.

**Duplicatas descartadas (R5).** 656 eventos duplicados por `event_id` foram
removidos (1 ocorrência mantida por `event_id`). Reconciliação conferida:
processados (64.372) = válidos (61.813) + duplicatas (656) + rejeitados distintos
(1.903).

**Rejeições por motivo (R7)** — 1.903 eventos distintos, um motivo por linha em
`silver/eventos_rejeitados`:

| Motivo | Eventos |
| --- | ---: |
| timestamp_ausente_ou_futuro | 948 |
| valor_monetario_invalido | 349 |
| marco_fora_do_padrao | 272 |
| evento_fora_catalogo | 271 |
| reembolso_sem_compra | 32 |
| transacao_sem_id | 31 |

**Regras que podem rejeitar dado legítimo.** Duas merecem atenção:
- `timestamp_ausente_ou_futuro` descarta eventos sem `event_ts` ou com data no
  futuro. Isso protege as métricas temporais, mas um relógio de cliente adiantado
  faria um evento real cair aqui. É o maior balde de rejeição (948).
- `marco_fora_do_padrao` rejeita `scroll`/vídeo com marco fora do conjunto
  esperado (ex.: `scroll` de 33%). São defeitos injetados pelo gerador, mas a
  mesma regra barraria um marco novo legítimo se o site passasse a emitir outros
  cortes de rolagem.

As demais (`valor_monetario_invalido`, `evento_fora_catalogo`,
`reembolso_sem_compra`, `transacao_sem_id`) correspondem a defeitos deliberados do
gerador e não removem dado legítimo.

**Armadilhas encontradas e convenções adotadas (design).**
- **`item_id` não é coluna de topo na Silver.** O plano de cada `select_item` vem
  de `items[0].item_id` (cada `select_item` carrega um único item no array
  `items`); o join `purchase → silver/itens` é sempre por `event_id`
  (`silver/itens` não tem `transaction_id`). `transaction_id` liga apenas
  `refund → purchase`.
- **Canal** = `coalesce(source,"") / coalesce(medium,"")` (coalesce **antes** do
  `concat_ws`), para o separador aparecer sempre e a chave de agrupamento não se
  deslocar entre grupos. **Dispositivo** = `device.category`.
- **Reembolso** é lançado na **data da compra** (LEFT JOIN purchase ⟕ refund pelas
  chaves herdadas da compra), garantindo `receita_bruta ≥ receita_reembolsada` em
  toda linha de `receita_diaria` (0 violações conferidas).
- **`saidas_downloads`** descarta linhas de **saída** com `link_domain` nulo (não
  há domínio a agrupar), mas **nunca** descarta downloads: estes vêm de
  `file_download`/`file_name` e independem de `link_domain`.
- **Modo ANSI do Spark 4**: todas as taxas com denominador possivelmente zero usam
  `try_divide` (retorna `null` em vez de abortar o job).
- **Tabela #13** é satisfeita pelo **par** `segmentos_visitantes` (um segmento por
  visitante) + `segmentos_perfil` (médias por segmento).

---

## Reprodutibilidade

```bash
docker compose up -d floci
docker compose run --rm app python gerador/gerar_eventos.py      # só se a landing estiver vazia
docker compose run --rm app python infra/verificar_ambiente.py   # 3 linhas [ok]
docker compose run --rm app python pipeline/executar_pipeline.py # rc=0; grava as 15 tabelas + site/mapa_calor.js
docker compose run --rm app python -m pytest                     # 436 passed
```

O mapa de calor (R11) é exportado para `site/mapa_calor.js` pelo 8º job e acende
sobre a landing de exemplo (`site/index.html` lê `window.MAPA_CALOR`).
