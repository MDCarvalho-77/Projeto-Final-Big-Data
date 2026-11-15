"""Exportador do mapa de calor (R11): gera ``site/mapa_calor.js``.

    docker compose run --rm app python pipeline/exportar_mapa_calor.py

Lê as duas tabelas Gold ``mapa_calor_cliques`` e ``mapa_calor_rolagem`` **pelo
Spark** (R1), traz as linhas para o Python com ``collect()`` (tabelas pequenas,
padrão do ``pipeline/README.md``) e grava **uma única linha** no único arquivo
que o pipeline tem permissão de escrever em disco (exceção do R11):

    window.MAPA_CALOR = {"gerado_em": "...Z", "cliques": [...], "rolagem": [...]};

O site (`site/index.html`) lê ``window.MAPA_CALOR``: por clique usa ``l.pagina``,
``l.element_id``, ``l.intensidade``, ``l.taxa_clique``; por rolagem ``l.pagina``,
``l.marco``, ``l.pct_sessoes``; e ``MAPA_CALOR.gerado_em``. Dados ausentes viram
listas vazias (JSON válido). O job é reexecutável: ``write_text`` sobrescreve.

Depende de ``pipeline/gold_mapa_calor.py``: se as tabelas não existirem, para com
mensagem clara. Usa só ``json``/``datetime`` + ``comum`` (sem ``boto3``).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import comum
from comum import arquivo_do_projeto, criar_sessao, titulo

TABELA_CLIQUES = "gold/mapa_calor_cliques"
TABELA_ROLAGEM = "gold/mapa_calor_rolagem"
DESTINO = "site/mapa_calor.js"


def _linhas(spark, tabela: str) -> list[dict]:
    """Lê uma tabela Gold pelo Spark e devolve as linhas como dicts.

    Tabela ausente/vazia → lista vazia (o site trata com ``|| []``)."""
    if not comum.existe(spark, comum.caminho(tabela)):
        return []
    return [linha.asDict() for linha in spark.read.parquet(comum.caminho(tabela)).collect()]


def montar_payload(cliques: list[dict], rolagem: list[dict], gerado_em: str) -> dict:
    """Monta o dicionário gravado em ``window.MAPA_CALOR``."""
    return {"gerado_em": gerado_em, "cliques": cliques, "rolagem": rolagem}


def renderizar(payload: dict) -> str:
    """Serializa o payload na linha única que o site espera (terminada em ``;\\n``).

    ``default=float`` cobre eventuais ``Decimal``; ``ensure_ascii=False`` preserva
    acentos. Nulos (``taxa_clique``/``pct_sessoes``) viram ``null`` no JSON."""
    texto = json.dumps(payload, ensure_ascii=False, default=float)
    return "window.MAPA_CALOR = " + texto + ";\n"


def main() -> int:
    spark = criar_sessao("exportar_mapa_calor")
    try:
        existe_cliques = comum.existe(spark, comum.caminho(TABELA_CLIQUES))
        existe_rolagem = comum.existe(spark, comum.caminho(TABELA_ROLAGEM))
        if not (existe_cliques and existe_rolagem):
            raise SystemExit(
                "gold/mapa_calor_* não existe. "
                "Rode antes: python pipeline/gold_mapa_calor.py"
            )

        cliques = _linhas(spark, TABELA_CLIQUES)
        rolagem = _linhas(spark, TABELA_ROLAGEM)
    finally:
        spark.stop()

    gerado_em = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = montar_payload(cliques, rolagem, gerado_em)

    destino = arquivo_do_projeto(DESTINO)
    destino.write_text(renderizar(payload), encoding="utf-8")

    titulo("Exportação do mapa de calor (R11)")
    print(f"[ok] gerado_em: {gerado_em}")
    print(f"[ok] cliques: {len(cliques)} itens | rolagem: {len(rolagem)} itens")
    print(f"[ok] Gravado: {destino}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
