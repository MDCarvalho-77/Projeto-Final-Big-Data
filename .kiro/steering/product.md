# Produto — DadosPro

## O que é o DadosPro

**DadosPro — Formação em Análise de Dados** é um curso online vendido por uma
landing page de conversão de página única. A página foi instrumentada com 20
eventos de tracking (ver `docs/CONTRATO_EVENTO.md`), que chegam ao bucket S3
`datalake-tracking` como arquivos JSON Lines no prefixo `landing/`.

Este repositório é o **projeto final de Engenharia de Dados**: construir, do
zero, um datalake em arquitetura medalhão (Bronze → Silver → Gold) que
transforma esses eventos brutos em respostas de negócio.

## Problema de negócio

Os eventos existem, mas ninguém os analisa ainda. A empresa precisa de respostas
para quatro perguntas (fonte: `docs/ENUNCIADO.md`, seção 1):

1. **Funil** — em que etapa a jornada de compra perde mais gente, por plano e por
   canal?
2. **Engajamento** — o vídeo de apresentação (VSL) é visto até o fim? O que o
   visitante procura no FAQ e não encontra?
3. **Receita e aquisição** — quanto entrou de fato, descontados os reembolsos, e
   qual canal traz cliente que não desiste?
4. **Previsão** — quais sessões estão perto de comprar, e que perfis de visitante
   existem?

## Catálogo de produtos (fonte: ENUNCIADO seção 2)

| Item | `item_id` | Preço | Observação |
| --- | --- | --- | --- |
| Plano Básico | `PLANO-BASICO` | R$ 297,00 | acesso por 1 ano |
| Plano Pro | `PLANO-PRO` | R$ 597,00 | acesso vitalício + mentoria |
| Plano Premium | `PLANO-PREMIUM` | R$ 997,00 | inclui apostila impressa (Correios) |
| Mentoria em grupo | `BUMP-MENTORIA` | R$ 147,00 | *order bump* no checkout |

Regras de negócio relevantes: frete só no Premium (PAC R$ 24,90 ou SEDEX R$ 49,90);
Pix tem 5% de desconto (cupom `PIX5`); `tax` é o ISS de 5% sobre `value`; moeda
única BRL.

## Usuários e resultados esperados

- **Usuário final das Golds:** o gerente/analista da empresa, que lê o
  `RELATORIO.md` e as 15 tabelas Gold para decidir onde investir.
- **Avaliador do projeto:** confere os requisitos R1–R11 e a rubrica de 100
  pontos (ENUNCIADO seção 7).

Entregáveis de negócio:

- 15 tabelas Gold (ENUNCIADO seção 5.3) que respondem às quatro perguntas.
- `RELATORIO.md` na raiz do projeto, com uma resposta por pergunta sustentada por
  tabela + número + recomendação, mais a seção obrigatória "Qualidade do dado"
  (duplicatas descartadas, rejeições por motivo, regras que podem ter rejeitado
  dado legítimo e armadilhas encontradas).
- Mapa de calor que acende no site de exemplo (R11), exportado para
  `site/mapa_calor.js`.

## O que não inventar

Todas as regras, campos, métricas e definições vêm de `docs/ENUNCIADO.md` e
`docs/CONTRATO_EVENTO.md`. Não criar métricas, tabelas ou resultados que não
estejam nesses documentos. Em caso de contradição ou dúvida, registrar para
revisão em vez de decidir sozinho.
