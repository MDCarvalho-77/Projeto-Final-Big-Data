"""Orquestrador do pipeline (R3): roda os 8 jobs em sequência, do Bronze ao
mapa de calor, sem gerar a landing.

    docker compose run --rm app python pipeline/executar_pipeline.py
    docker compose run --rm app python pipeline/executar_pipeline.py --desde=silver
    docker compose run --rm app python pipeline/executar_pipeline.py --desde=bronze --recriar-bronze

Sequência oficial (design §8.1), sem paralelismo:

    bronze → silver → gold_funil → gold_engajamento → gold_receita_aquisicao →
    gold_ml → gold_mapa_calor → exportar_mapa_calor

Cada job expõe ``main() -> int`` (padrão de bronze.py/silver.py) e cria/fecha a
própria SparkSession. O orquestrador os invoca no MESMO processo, em ordem,
medindo tempo e capturando ``SystemExit``/exceção; para na primeira falha
(rc != 0) e propaga o código de saída ao shell (design §8.1/§8.3).

Bronze append-only (design §8.2). A Bronze faz ``mode("append")``: reexecutar a
sequência sobre uma Bronze já populada reinseriria os mesmos eventos. Para não
duplicar, o orquestrador detecta se a Bronze já existe e, por padrão, PULA a
ingestão Bronze quando ela está populada — sem apagar histórico. Três modos:

- ``--desde=auto`` (padrão): se a Bronze já existe, pula a etapa Bronze e começa
  pela Silver; senão, ingere a Bronze normalmente.
- ``--desde=bronze --recriar-bronze``: apaga a Bronze (``apagar_camada``) e
  reingere do zero. Só sob a flag explícita — nunca apaga histórico em silêncio.
- ``--desde=<etapa>``: começa explicitamente na etapa indicada (as dependências
  entre camadas são garantidas pelo ``exigir_camada`` de cada job, R2).

O orquestrador NUNCA chama o gerador (R3 "não gera dados"): a landing é
pré-condição; se faltar, o ``exigir_camada`` da Bronze orienta rodar o gerador.

Reaproveita pipeline/comum.py (R1/R2): não reimplementa sessão nem acesso ao S3.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone

import bronze
import comum
import exportar_mapa_calor
import gold_engajamento
import gold_funil
import gold_mapa_calor
import gold_ml
import gold_receita_aquisicao
import silver
from comum import titulo

# Sequência oficial dos 8 jobs (design §8.1). Ordem FIXA; cada par é (nome, main).
ETAPAS = [
    ("bronze", bronze.main),
    ("silver", silver.main),
    ("gold_funil", gold_funil.main),
    ("gold_engajamento", gold_engajamento.main),
    ("gold_receita_aquisicao", gold_receita_aquisicao.main),
    ("gold_ml", gold_ml.main),
    ("gold_mapa_calor", gold_mapa_calor.main),
    ("exportar_mapa_calor", exportar_mapa_calor.main),
]

NOMES_ETAPAS = [nome for nome, _ in ETAPAS]


def _agora_utc() -> str:
    """Instante atual em UTC, formato ISO com sufixo Z (para o log por etapa)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_args(argv):
    """Interpreta os argumentos de linha de comando.

    Devolve (desde, recriar_bronze). ``desde`` é ``"auto"`` ou o nome de uma
    etapa de ETAPAS. ``recriar_bronze`` é bool. Argumentos inválidos param o
    orquestrador com SystemExit e mensagem clara.
    """
    desde = "auto"
    recriar_bronze = False
    for arg in argv:
        if arg == "--recriar-bronze":
            recriar_bronze = True
        elif arg.startswith("--desde="):
            desde = arg[len("--desde="):]
        else:
            raise SystemExit(
                f"Argumento desconhecido: {arg}. "
                f"Use --desde=auto|{'|'.join(NOMES_ETAPAS)} e/ou --recriar-bronze."
            )

    if desde != "auto" and desde not in NOMES_ETAPAS:
        raise SystemExit(
            f"--desde={desde} inválido. Valores: auto, {', '.join(NOMES_ETAPAS)}."
        )
    if recriar_bronze and desde not in ("auto", "bronze"):
        raise SystemExit(
            "--recriar-bronze só vale começando pela Bronze "
            "(use --desde=bronze --recriar-bronze ou apenas --recriar-bronze)."
        )
    return desde, recriar_bronze


def _preparar_bronze(desde: str, recriar_bronze: bool):
    """Aplica a estratégia Bronze append-only (design §8.2) ANTES das etapas.

    Abre uma SparkSession utilitária só para checar/limpar a camada Bronze e a
    fecha em seguida (cada etapa abre a sua própria sessão). Devolve o nome da
    etapa inicial efetiva:

    - ``--recriar-bronze``: apaga a Bronze (intenção explícita) e começa em
      ``bronze`` para reingerir do zero. Nunca apaga histórico em silêncio.
    - ``auto``: se a Bronze já existe, começa na Silver (PULA a Bronze, não
      duplica); senão, começa na Bronze para ingeri-la.
    - ``--desde=<etapa>`` explícito: respeita a etapa pedida sem mexer na Bronze.
    """
    if desde == "auto" or recriar_bronze:
        spark = comum.criar_sessao("orquestrador")
        try:
            existe = comum.camada_existe(spark, "bronze")
            if recriar_bronze:
                if comum.apagar_camada(spark, "bronze"):
                    print("[info] Bronze apagada a pedido de --recriar-bronze; será reingerida.")
                else:
                    print("[info] --recriar-bronze: Bronze não existia; será ingerida.")
                return "bronze"
            # desde == "auto"
            if existe:
                print("[info] Bronze já existe (append-only): pulando a ingestão Bronze e "
                      "começando pela Silver. Use --recriar-bronze para reingerir.")
                return "silver"
            print("[info] Bronze ainda não existe: ingerindo a Bronze a partir da landing.")
            return "bronze"
        finally:
            spark.stop()
    # --desde=<etapa> explícito: não mexe na Bronze.
    return desde


def executar(argv=None) -> int:
    """Roda a sequência dos 8 jobs a partir da etapa inicial calculada.

    Retorna 0 em sucesso; o rc da primeira etapa que falhar, caso contrário.
    """
    desde, recriar_bronze = _parse_args(argv if argv is not None else sys.argv[1:])
    inicial = _preparar_bronze(desde, recriar_bronze)

    comeco = NOMES_ETAPAS.index(inicial)
    etapas = ETAPAS[comeco:]
    titulo(f"Pipeline DadosPro — {len(etapas)} etapa(s) a partir de '{inicial}'")

    executadas = []
    for nome, fn in etapas:
        titulo(f"Etapa: {nome} — início {_agora_utc()}")
        t0 = time.monotonic()
        try:
            rc = fn()
        except SystemExit as e:  # exigir_camada/validações dos jobs usam SystemExit
            rc = e.code if isinstance(e.code, int) else 1
        except Exception as e:  # noqa: BLE001 — qualquer falha de etapa interrompe a sequência
            print(f"[FALHA] {nome}: {e!r}")
            rc = 1
        dur = time.monotonic() - t0

        if rc != 0:
            print(f"[FALHA] {nome} (rc={rc}) — fim {_agora_utc()} ({dur:.1f}s); "
                  "interrompendo a sequência.")
            executadas.append((nome, rc))
            _resumo(executadas)
            return rc
        print(f"[ok] {nome} concluída — fim {_agora_utc()} ({dur:.1f}s)")
        executadas.append((nome, 0))

    _resumo(executadas)
    return 0


def _resumo(executadas) -> None:
    """Imprime o resumo final com o status de cada etapa executada."""
    titulo("Resumo do pipeline")
    for nome, rc in executadas:
        print(f"  {'[ok]  ' if rc == 0 else '[FALHA]'} {nome}" + ("" if rc == 0 else f" (rc={rc})"))


def main() -> int:
    return executar()


if __name__ == "__main__":
    raise SystemExit(main())
