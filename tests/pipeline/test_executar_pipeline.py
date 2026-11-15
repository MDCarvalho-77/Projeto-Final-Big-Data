"""Testes do orquestrador (pipeline/executar_pipeline.py, R3).

Não usam Spark real: monkeypatcham as funções ``main`` de cada módulo de job e as
funções de camada de ``comum`` (``criar_sessao``, ``camada_existe``,
``apagar_camada``). Assim os testes verificam só a orquestração — ordem das 8
etapas, parada na primeira falha com propagação do rc, estratégia Bronze
append-only (modo auto pula/ingere; --recriar-bronze apaga) e a garantia de que
o gerador nunca é chamado.
"""

from __future__ import annotations

import pytest

import comum
import executar_pipeline as orq


class _SessaoFalsa:
    """Stand-in de SparkSession: registra que stop() foi chamado."""

    def __init__(self):
        self.parado = False

    def stop(self):
        self.parado = True


@pytest.fixture
def espionar(monkeypatch):
    """Substitui cada etapa por um main() que só registra a chamada.

    Devolve um dict com a ordem de chamadas ('chamadas') e permite configurar o
    rc de cada etapa por nome ('rc'). Também controla a existência/limpeza da
    Bronze sem Spark.
    """
    estado = {
        "chamadas": [],
        "rc": {},
        "bronze_existe": False,
        "apagou_bronze": False,
        "sessao": _SessaoFalsa(),
    }

    def _fazer_main(nome):
        def _main():
            estado["chamadas"].append(nome)
            return estado["rc"].get(nome, 0)
        return _main

    # Substitui o main de cada etapa dentro da lista ETAPAS do orquestrador.
    novas_etapas = [(nome, _fazer_main(nome)) for nome, _ in orq.ETAPAS]
    monkeypatch.setattr(orq, "ETAPAS", novas_etapas)
    monkeypatch.setattr(orq, "NOMES_ETAPAS", [n for n, _ in novas_etapas])

    # Camada Bronze sem Spark.
    monkeypatch.setattr(comum, "criar_sessao", lambda nome: estado["sessao"])
    monkeypatch.setattr(comum, "camada_existe", lambda spark, camada: estado["bronze_existe"])

    def _apagar(spark, camada):
        estado["apagou_bronze"] = True
        return estado["bronze_existe"]

    monkeypatch.setattr(comum, "apagar_camada", _apagar)
    return estado


ORDEM_ESPERADA = [
    "bronze",
    "silver",
    "gold_funil",
    "gold_engajamento",
    "gold_receita_aquisicao",
    "gold_ml",
    "gold_mapa_calor",
    "exportar_mapa_calor",
]


def test_ordem_das_oito_etapas(espionar):
    """Com a Bronze inexistente e modo padrão, roda as 8 etapas na ordem oficial."""
    rc = orq.executar([])
    assert rc == 0
    assert espionar["chamadas"] == ORDEM_ESPERADA


def test_para_na_primeira_falha_e_propaga_rc(espionar):
    """Falha numa etapa interrompe a sequência e propaga o rc; etapas seguintes não rodam."""
    espionar["rc"]["gold_funil"] = 3
    rc = orq.executar([])
    assert rc == 3
    # Parou logo após gold_funil: nada depois dele foi chamado.
    assert espionar["chamadas"] == ["bronze", "silver", "gold_funil"]


def test_excecao_em_etapa_interrompe_com_rc_nao_zero(espionar, monkeypatch):
    """Exceção numa etapa é capturada, vira falha (rc != 0) e interrompe a sequência."""
    def _explode():
        raise RuntimeError("boom")

    etapas = []
    for nome, fn in orq.ETAPAS:
        etapas.append((nome, _explode if nome == "silver" else fn))
    monkeypatch.setattr(orq, "ETAPAS", etapas)

    rc = orq.executar([])
    assert rc != 0
    assert espionar["chamadas"] == ["bronze"]


def test_modo_auto_pula_bronze_quando_existe(espionar):
    """Modo auto: Bronze já existe → pula a ingestão e começa pela Silver (não duplica)."""
    espionar["bronze_existe"] = True
    rc = orq.executar([])
    assert rc == 0
    assert espionar["chamadas"] == ORDEM_ESPERADA[1:]  # sem 'bronze'
    assert espionar["apagou_bronze"] is False


def test_modo_auto_ingere_bronze_quando_nao_existe(espionar):
    """Modo auto: Bronze inexistente → ingere a Bronze (primeira etapa)."""
    espionar["bronze_existe"] = False
    rc = orq.executar([])
    assert rc == 0
    assert espionar["chamadas"][0] == "bronze"
    assert espionar["chamadas"] == ORDEM_ESPERADA


def test_recriar_bronze_apaga_e_reingere(espionar):
    """--recriar-bronze chama apagar_camada e começa pela Bronze mesmo se já existe."""
    espionar["bronze_existe"] = True
    rc = orq.executar(["--desde=bronze", "--recriar-bronze"])
    assert rc == 0
    assert espionar["apagou_bronze"] is True
    assert espionar["chamadas"] == ORDEM_ESPERADA


def test_desde_etapa_explicita_nao_mexe_na_bronze(espionar):
    """--desde=<etapa> começa na etapa indicada e não checa/apaga a Bronze."""
    rc = orq.executar(["--desde=gold_ml"])
    assert rc == 0
    assert espionar["chamadas"] == ["gold_ml", "gold_mapa_calor", "exportar_mapa_calor"]
    assert espionar["apagou_bronze"] is False


def test_nunca_chama_o_gerador(espionar, monkeypatch):
    """O orquestrador jamais importa/chama o gerador de eventos (R3 não gera dados)."""
    import sys

    chamado = {"gerador": False}

    # Qualquer tentativa de usar o gerador seria detectada por um atributo sentinela.
    class _GeradorProibido:
        def __getattr__(self, _):
            chamado["gerador"] = True
            raise AssertionError("o orquestrador não deve chamar o gerador")

    monkeypatch.setitem(sys.modules, "gerar_eventos", _GeradorProibido())
    orq.executar([])
    assert chamado["gerador"] is False


def test_argumento_desconhecido_para_com_erro(espionar):
    """Argumento inválido interrompe com SystemExit e não executa etapa alguma."""
    with pytest.raises(SystemExit):
        orq.executar(["--xpto"])
    assert espionar["chamadas"] == []


def test_desde_invalido_para_com_erro(espionar):
    """--desde com valor fora da lista de etapas interrompe com SystemExit."""
    with pytest.raises(SystemExit):
        orq.executar(["--desde=inexistente"])
    assert espionar["chamadas"] == []
