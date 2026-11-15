"""Testes do exportador do mapa de calor (pipeline/exportar_mapa_calor.py).

Seguem o padrão dos demais testes de pipeline: fixture `spark` (local[2]),
`comum.caminho` apontado para `file://{tmp_path}` e `comum.arquivo_do_projeto`
redirecionado para a pasta temporária. `exportar_mapa_calor.criar_sessao` é
trocado pela própria fixture para o `main()` não subir uma SparkSession nova.

Cobrem o contrato do R11 com o site:
- o arquivo começa com `window.MAPA_CALOR = {` e termina com `};`;
- o corpo é JSON parseável;
- cada item de cliques/rolagem tem exatamente as chaves exigidas;
- dados ausentes -> `"cliques": []` e `"rolagem": []` (JSON válido);
- reexecução sobrescreve o arquivo;
- `gerado_em` no formato `...Z`.
"""

from __future__ import annotations

import json
import re

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

import comum
import exportar_mapa_calor


_SCHEMA_CLIQUES = StructType([
    StructField("pagina", StringType()),
    StructField("element_id", StringType()),
    StructField("cliques", LongType()),
    StructField("sessoes", LongType()),
    StructField("sessoes_na_pagina", LongType()),
    StructField("taxa_clique", DoubleType()),
    StructField("intensidade", DoubleType()),
])

_SCHEMA_ROLAGEM = StructType([
    StructField("pagina", StringType()),
    StructField("marco", IntegerType()),
    StructField("sessoes_que_chegaram", LongType()),
    StructField("pct_sessoes", DoubleType()),
])


def _preparar(spark, monkeypatch, tmp_path, cliques, rolagem, escrever_cliques=True, escrever_rolagem=True):
    """Aponta comum para file://{tmp_path}, grava as tabelas Gold e redireciona a
    saída do exportador para a pasta temporária. Devolve o caminho do destino."""
    monkeypatch.setattr(comum, "caminho", lambda camada: f"file://{tmp_path}/{camada}")
    monkeypatch.setattr(exportar_mapa_calor, "criar_sessao", lambda nome: spark)
    # main() chama spark.stop() no finally; preservar a sessão compartilhada da fixture
    monkeypatch.setattr(spark, "stop", lambda: None)
    destino = tmp_path / "site" / "mapa_calor.js"
    destino.parent.mkdir(parents=True, exist_ok=True)   # no repo real, site/ já existe
    monkeypatch.setattr(exportar_mapa_calor, "arquivo_do_projeto", lambda rel: destino)

    if escrever_cliques:
        (spark.createDataFrame(cliques, schema=_SCHEMA_CLIQUES)
            .write.mode("overwrite").parquet(f"file://{tmp_path}/gold/mapa_calor_cliques"))
    if escrever_rolagem:
        (spark.createDataFrame(rolagem, schema=_SCHEMA_ROLAGEM)
            .write.mode("overwrite").parquet(f"file://{tmp_path}/gold/mapa_calor_rolagem"))
    return destino


def _parse(destino):
    """Lê o arquivo gerado, valida o envelope e devolve o dict parseado."""
    texto = destino.read_text(encoding="utf-8")
    assert texto.startswith("window.MAPA_CALOR = {")
    assert texto.rstrip("\n").endswith("};")
    corpo = texto.strip()[len("window.MAPA_CALOR = "):].rstrip(";\n")
    return json.loads(corpo)


# --------------------------------------------------------------------------- #
# Envelope e JSON
# --------------------------------------------------------------------------- #

def test_arquivo_envelope_e_json_parseavel(spark, tmp_path, monkeypatch):
    destino = _preparar(
        spark, monkeypatch, tmp_path,
        cliques=[("/", "cta", 3, 2, 2, 1.0, 1.0)],
        rolagem=[("/", 0, 2, 1.0)],
    )
    assert exportar_mapa_calor.main() == 0
    dados = _parse(destino)
    assert set(dados.keys()) == {"gerado_em", "cliques", "rolagem"}


def test_chaves_exatas_por_item(spark, tmp_path, monkeypatch):
    destino = _preparar(
        spark, monkeypatch, tmp_path,
        cliques=[("/", "cta", 3, 2, 2, 1.0, 1.0)],
        rolagem=[("/", 25, 1, 0.5)],
    )
    exportar_mapa_calor.main()
    dados = _parse(destino)
    assert set(dados["cliques"][0].keys()) == {
        "pagina", "element_id", "cliques", "sessoes",
        "sessoes_na_pagina", "taxa_clique", "intensidade",
    }
    assert set(dados["rolagem"][0].keys()) == {
        "pagina", "marco", "sessoes_que_chegaram", "pct_sessoes",
    }


def test_gerado_em_formato_z(spark, tmp_path, monkeypatch):
    destino = _preparar(
        spark, monkeypatch, tmp_path,
        cliques=[("/", "cta", 1, 1, 1, 1.0, 1.0)],
        rolagem=[("/", 0, 1, 1.0)],
    )
    exportar_mapa_calor.main()
    dados = _parse(destino)
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", dados["gerado_em"])


def test_taxa_nula_vira_null_no_json(spark, tmp_path, monkeypatch):
    destino = _preparar(
        spark, monkeypatch, tmp_path,
        cliques=[("/outra", "x", 1, 1, None, None, 1.0)],
        rolagem=[("/", 0, 1, 1.0)],
    )
    exportar_mapa_calor.main()
    dados = _parse(destino)
    assert dados["cliques"][0]["taxa_clique"] is None


# --------------------------------------------------------------------------- #
# Dados ausentes e reexecução
# --------------------------------------------------------------------------- #

def test_dados_ausentes_geram_listas_vazias_validas(spark, tmp_path, monkeypatch):
    # tabelas existem mas estão vazias -> cliques:[] e rolagem:[] (JSON válido)
    destino = _preparar(spark, monkeypatch, tmp_path, cliques=[], rolagem=[])
    assert exportar_mapa_calor.main() == 0
    dados = _parse(destino)
    assert dados["cliques"] == []
    assert dados["rolagem"] == []


def test_reexecucao_sobrescreve(spark, tmp_path, monkeypatch):
    destino = _preparar(
        spark, monkeypatch, tmp_path,
        cliques=[("/", "cta", 3, 2, 2, 1.0, 1.0)],
        rolagem=[("/", 0, 2, 1.0)],
    )
    exportar_mapa_calor.main()
    primeiro = destino.read_text(encoding="utf-8")
    assert "cta" in primeiro

    # regrava a tabela de cliques com outro elemento e reexecuta
    (spark.createDataFrame([("/", "outro", 1, 1, 1, 1.0, 1.0)], schema=_SCHEMA_CLIQUES)
        .write.mode("overwrite").parquet(f"file://{tmp_path}/gold/mapa_calor_cliques"))
    exportar_mapa_calor.main()
    segundo = destino.read_text(encoding="utf-8")
    assert "outro" in segundo
    assert "cta" not in segundo
    # continua sendo uma única linha com o envelope correto
    assert segundo.startswith("window.MAPA_CALOR = {")
    assert segundo.rstrip("\n").endswith("};")


def test_tabela_ausente_interrompe_com_mensagem(spark, tmp_path, monkeypatch):
    # grava só cliques; rolagem ausente -> SystemExit com instrução
    _preparar(
        spark, monkeypatch, tmp_path,
        cliques=[("/", "cta", 1, 1, 1, 1.0, 1.0)],
        rolagem=[],
        escrever_rolagem=False,
    )
    with pytest.raises(SystemExit) as exc:
        exportar_mapa_calor.main()
    assert "gold_mapa_calor.py" in str(exc.value)
