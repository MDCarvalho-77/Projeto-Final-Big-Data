"""Testes do job Silver (pipeline/silver.py).

Seguem o padrão de tests/pipeline/test_bronze.py e test_comum.py: usam a fixture
`spark` (local[2]), trocam comum.caminho por um caminho file:// numa pasta
temporária, e escrevem a Bronze no schema real (com params_json, params,
proveniência e partições event_name=.../year=.../month=.../day=...) reaproveitando
bronze.construir_bronze a partir de JSONL de landing.

Cobrem os seis grupos de requisitos:
- R5 deduplicação (incl. event_id nulo não-duplicata);
- R6 tipagem de params + element_id (incl. discriminante numérico/texto/ausente/null);
- R7 validação com motivo (uma por regra + aceitos; 2 passos; um motivo por linha);
- R8 sessionização (corte de 30 min; sem session_hint; ordem determinística);
- R9 identity stitching (incl. login rejeitado que ainda costura; join 1:1);
- R10 itens explodidos (0/1/vários; schema; integridade referencial);
mais `pagina`, integração gravação/releitura, preservação das vizinhas,
idempotência, reconciliação e exigir_camada.
"""

from __future__ import annotations

import json

import pytest

import bronze
import comum
import silver
from pyspark.sql import functions as F


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _apontar_para_local(monkeypatch, tmp_path):
    monkeypatch.setattr(comum, "caminho", lambda camada: f"file://{tmp_path}/{camada}")


def _escrever_landing(tmp_path, event_name, year, month, day, eventos, arquivo="coletor-s42-001.jsonl"):
    pasta = tmp_path / "landing" / f"event_name={event_name}" / f"year={year}" / f"month={month}" / f"day={day}"
    pasta.mkdir(parents=True, exist_ok=True)
    linhas = "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in eventos)
    (pasta / arquivo).write_text(linhas, encoding="utf-8")


def _evento(event_name, event_id, event_ts="2026-09-01T10:00:00.000Z",
            user_pseudo_id="anon-1", user_id=None, session_hint="s-1",
            params=None, items=None, location="https://loja.exemplo/"):
    """Um evento genérico nos moldes do contrato."""
    return {
        "event_id": event_id,
        "event_name": event_name,
        "event_ts": event_ts,
        "user_pseudo_id": user_pseudo_id,
        "user_id": user_id,
        "session_hint": session_hint,
        "page": {"location": location, "referrer": "https://loja.exemplo/"},
        "traffic": {"source": "google", "medium": "cpc", "campaign": "c"},
        "device": {"category": "mobile", "os": "Android", "browser": "Chrome"},
        "params": params if params is not None else {},
        "items": items if items is not None else [],
    }


def _bronze_de(spark, tmp_path, por_evento):
    """Escreve a landing (um arquivo por event_name) e constrói a Bronze real.

    `por_evento` é um dict {event_name: [eventos...]}. Para forçar proveniência
    distinta (dedup), passar tuplas (lista, arquivo) por event_name."""
    for event_name, eventos in por_evento.items():
        arquivo = "coletor-s42-001.jsonl"
        if isinstance(eventos, tuple):
            eventos, arquivo = eventos
        _escrever_landing(tmp_path, event_name, "2026", "09", "01", eventos, arquivo=arquivo)
    return bronze.construir_bronze(spark)


def _ts(dia_hora):
    """Atalho ISO-8601 UTC: _ts('10:00') -> 2026-09-01T10:00:00.000Z."""
    return f"2026-09-01T{dia_hora}:00.000Z"


# --------------------------------------------------------------------------- #
# R5 — Deduplicação
# --------------------------------------------------------------------------- #

def test_r5_duas_copias_exatas_viram_uma(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    # mesma event_id em dois arquivos (reenvio de lote)
    _escrever_landing(tmp_path, "page_view", "2026", "09", "01",
                      [_evento("page_view", "e1", params={"page_title": "home"})],
                      arquivo="coletor-s42-001.jsonl")
    _escrever_landing(tmp_path, "page_view", "2026", "09", "01",
                      [_evento("page_view", "e1", params={"page_title": "home"})],
                      arquivo="coletor-s42-002.jsonl")
    bronze_df = bronze.construir_bronze(spark)
    assert bronze_df.count() == 2

    dedup = silver.deduplicar(bronze_df)
    assert dedup.count() == 1
    linha = dedup.collect()[0]
    # proveniência preservada: menor _arquivo_origem vence
    assert linha[silver.COL_ARQUIVO_ORIGEM].endswith("coletor-s42-001.jsonl")


def test_r5_eventos_distintos_semelhantes_nao_deduplicam(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    bronze_df = _bronze_de(spark, tmp_path, {
        "view_item": [
            _evento("view_item", "e1", params={"value": 597.0, "currency": "BRL"}),
            _evento("view_item", "e2", params={"value": 597.0, "currency": "BRL"}),
        ],
    })
    dedup = silver.deduplicar(bronze_df)
    assert dedup.count() == 2


def test_r5_determinismo_ordem_de_entrada_nao_muda_sobrevivente(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    _escrever_landing(tmp_path, "page_view", "2026", "09", "01",
                      [_evento("page_view", "e1", params={"page_title": "a"})],
                      arquivo="coletor-s42-003.jsonl")
    _escrever_landing(tmp_path, "page_view", "2026", "09", "01",
                      [_evento("page_view", "e1", params={"page_title": "a"})],
                      arquivo="coletor-s42-001.jsonl")
    bronze_df = bronze.construir_bronze(spark)
    dedup = silver.deduplicar(bronze_df)
    assert dedup.count() == 1
    # independentemente da ordem de leitura, sobrevive o menor arquivo
    assert dedup.collect()[0][silver.COL_ARQUIVO_ORIGEM].endswith("coletor-s42-001.jsonl")


def test_r5_event_id_nulo_nao_e_duplicata(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    bronze_df = _bronze_de(spark, tmp_path, {
        "page_view": [
            _evento("page_view", None, params={"page_title": "a"}),
            _evento("page_view", None, params={"page_title": "b"}),
        ],
        "scroll": (  # um par de cópias exatas com event_id não-nulo
            [
                _evento("scroll", "e-dup", params={"percent_scrolled": 50}),
            ], "coletor-s42-001.jsonl"),
    })
    # acrescenta a cópia do e-dup em outro arquivo
    _escrever_landing(tmp_path, "scroll", "2026", "09", "01",
                      [_evento("scroll", "e-dup", params={"percent_scrolled": 50})],
                      arquivo="coletor-s42-002.jsonl")
    bronze_df = bronze.construir_bronze(spark)

    dedup = silver.deduplicar(bronze_df)
    nulos = dedup.where(F.col("event_id").isNull()).count()
    assert nulos == 2  # os dois sem event_id sobrevivem isolados
    assert dedup.where(F.col("event_id") == "e-dup").count() == 1  # dedup normal
    # duplicatas removidas conta só a cópia do e-dup (1), não os nulos
    duplicatas = bronze_df.count() - dedup.count()
    assert duplicatas == 1


# --------------------------------------------------------------------------- #
# R6 — Tipagem de params + element_id
# --------------------------------------------------------------------------- #

def _tipar(spark, tmp_path, por_evento):
    bronze_df = _bronze_de(spark, tmp_path, por_evento)
    return silver.tipar_params(silver.deduplicar(bronze_df))


def test_r6_percent_scrolled_vira_inteiro(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    df = _tipar(spark, tmp_path, {"scroll": [_evento("scroll", "e1", params={"percent_scrolled": 50})]})
    linha = df.collect()[0]
    assert linha.percent_scrolled == 50
    assert dict(df.dtypes)["percent_scrolled"] == "int"


def test_r6_outbound_vira_booleano(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    df = _tipar(spark, tmp_path, {"click": [
        _evento("click", "e1", params={"link_url": "x", "outbound": True, "element_id": "social-instagram"})]})
    linha = df.collect()[0]
    assert linha.outbound is True
    assert dict(df.dtypes)["outbound"] == "boolean"


def test_r6_value_vira_double(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    df = _tipar(spark, tmp_path, {"view_item": [
        _evento("view_item", "e1", params={"value": 1086.8, "currency": "BRL"})]})
    linha = df.collect()[0]
    assert linha.value == 1086.8
    assert dict(df.dtypes)["value"] == "double"


def test_r6_chave_ausente_vira_nulo_sem_rejeicao(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    df = _tipar(spark, tmp_path, {"page_view": [_evento("page_view", "e1", params={"page_title": "home"})]})
    linha = df.collect()[0]
    assert linha.value is None
    assert linha.tem_value is False


def test_r6_discriminante_value_numerico_texto_ausente_null(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    # Monta params_json manualmente para controlar numérico/texto/ausente/null.
    _escrever_landing(tmp_path, "view_item", "2026", "09", "01", [
        _evento("view_item", "e-num", params={"value": 10.0, "currency": "BRL"}),
        _evento("view_item", "e-ausente", params={"currency": "BRL"}),
    ])
    # value textual e value:null precisam de JSON cru; escrevemos linha a linha
    pasta = tmp_path / "landing" / "event_name=view_item" / "year=2026" / "month=09" / "day=01"
    base = _evento("view_item", "e-texto")
    base["params"] = {"value": "abc", "currency": "BRL"}
    linha_null = _evento("view_item", "e-null")
    linha_null_json = json.dumps(linha_null)
    # injeta "value": null manualmente
    linha_null["params"] = {"currency": "BRL"}
    conteudo = json.dumps(base, ensure_ascii=False) + "\n"
    conteudo += '{"event_id":"e-null","event_name":"view_item","event_ts":"2026-09-01T10:00:00.000Z",' \
                '"user_pseudo_id":"anon-1","user_id":null,"session_hint":"s","page":{"location":"https://x/","referrer":null},' \
                '"traffic":{"source":"google","medium":"cpc","campaign":"c"},' \
                '"device":{"category":"mobile","os":"Android","browser":"Chrome"},' \
                '"params":{"value":null,"currency":"BRL"},"items":[]}\n'
    (pasta / "coletor-s42-002.jsonl").write_text(conteudo, encoding="utf-8")

    df = silver.tipar_params(silver.deduplicar(bronze.construir_bronze(spark)))
    por_id = {l.event_id: l for l in df.collect()}
    # numérico: tem_value true, tipado preenchido
    assert por_id["e-num"].tem_value is True and por_id["e-num"].value == 10.0
    # textual: tem_value true, tipado null -> base de valor_nao_numerico
    assert por_id["e-texto"].tem_value is True and por_id["e-texto"].value is None
    # ausente: tem_value false
    assert por_id["e-ausente"].tem_value is False
    # JSON null: tem_value false (colapsa com ausência)
    assert por_id["e-null"].tem_value is False


def test_r6_element_id_presente_no_click_ausente_no_scroll(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    df = _tipar(spark, tmp_path, {
        "click": [_evento("click", "e-click", params={"link_url": "x", "outbound": False, "element_id": "cta-logo"})],
        "scroll": [_evento("scroll", "e-scroll", params={"percent_scrolled": 25})],
    })
    por_id = {l.event_id: l for l in df.collect()}
    assert por_id["e-click"].element_id == "cta-logo"
    assert por_id["e-scroll"].element_id is None


# --------------------------------------------------------------------------- #
# event_ts parsing (R7/R8 dependem)
# --------------------------------------------------------------------------- #

def test_event_ts_parsing_com_e_sem_millis_e_lixo(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    _escrever_landing(tmp_path, "page_view", "2026", "09", "01", [
        _evento("page_view", "e-millis", event_ts="2026-10-03T14:22:05.123Z", params={"page_title": "a"}),
        _evento("page_view", "e-sem-millis", event_ts="2026-10-03T14:22:05Z", params={"page_title": "a"}),
        _evento("page_view", "e-lixo", event_ts="abc", params={"page_title": "a"}),
        _evento("page_view", "e-nulo", event_ts=None, params={"page_title": "a"}),
    ])
    df = silver.tipar_params(silver.deduplicar(bronze.construir_bronze(spark)))
    por_id = {l.event_id: l for l in df.collect()}
    assert por_id["e-millis"].event_ts_ts is not None
    assert por_id["e-sem-millis"].event_ts_ts is not None
    assert por_id["e-lixo"].event_ts_ts is None
    assert por_id["e-nulo"].event_ts_ts is None


# --------------------------------------------------------------------------- #
# R7 — Validação (passo 1)
# --------------------------------------------------------------------------- #

def _validar(spark, tmp_path, por_evento):
    bronze_df = _bronze_de(spark, tmp_path, por_evento)
    tipado = silver.normalizar_pagina(silver.tipar_params(silver.deduplicar(bronze_df)))
    classificado = silver.marcar_refund_orfao(silver.classificar(tipado))
    return silver.separar_validos_e_rejeitados(classificado)


def _motivos_de(rejeitados, event_id):
    return sorted(l.motivo_rejeicao for l in rejeitados.where(F.col("event_id") == event_id).collect())


def test_r7_fora_do_catalogo(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, rej = _validar(spark, tmp_path, {
        "add_to_wishlist": [_evento("add_to_wishlist", "e-fora", params={})],
    })
    assert _motivos_de(rej, "e-fora") == [silver.MOTIVO_FORA_CATALOGO]
    assert validos.count() == 0


def test_r7_event_name_vazio_hive_default_fora_catalogo(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, rej = _validar(spark, tmp_path, {
        "__HIVE_DEFAULT_PARTITION__": [_evento("__HIVE_DEFAULT_PARTITION__", "e-vazio", params={})],
    })
    assert _motivos_de(rej, "e-vazio") == [silver.MOTIVO_FORA_CATALOGO]
    # a partição __HIVE_DEFAULT_PARTITION__ é relida como event_name null pelo
    # Spark (sentinela de valor vazio); o evento é preservado na linha rejeitada
    linha = rej.where(F.col("event_id") == "e-vazio").collect()[0]
    assert linha.event_name is None


def test_r7_sem_event_id_e_sem_event_name_combinado(spark, tmp_path, monkeypatch):
    """BR-3: evento sem event_id E sem event_name (__HIVE_DEFAULT_PARTITION__)
    cai em evento_fora_catalogo, com classificação determinística e estável."""
    _apontar_para_local(monkeypatch, tmp_path)
    validos, rej = _validar(spark, tmp_path, {
        "__HIVE_DEFAULT_PARTITION__": [_evento("__HIVE_DEFAULT_PARTITION__", None, params={})],
    })
    linhas = rej.where(F.col("event_id").isNull()).collect()
    # motivos estáveis: cai em evento_fora_catalogo (event_name fora do catálogo)
    motivos = sorted(l.motivo_rejeicao for l in linhas)
    assert silver.MOTIVO_FORA_CATALOGO in motivos
    # não some na reconciliação: conta como 1 evento
    assert silver._contar_rejeitados_distintos(rej) == 1
    assert validos.count() == 0


def test_r7_timestamp_nulo_e_futuro(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, rej = _validar(spark, tmp_path, {
        "page_view": [
            _evento("page_view", "e-ts-nulo", event_ts=None, params={"page_title": "a"}),
            _evento("page_view", "e-ts-futuro", event_ts="2099-01-01T00:00:00.000Z", params={"page_title": "a"}),
        ],
    })
    assert _motivos_de(rej, "e-ts-nulo") == [silver.MOTIVO_TIMESTAMP]
    assert _motivos_de(rej, "e-ts-futuro") == [silver.MOTIVO_TIMESTAMP]


def test_r7_colunas_de_tempo_no_rejeitado(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    _, rej = _validar(spark, tmp_path, {
        "page_view": [_evento("page_view", "e-ts-nulo", event_ts=None, params={"page_title": "a"})],
    })
    linha = rej.where(F.col("event_id") == "e-ts-nulo").collect()[0]
    assert linha.event_ts is None            # cru
    assert linha.event_ts_ts is None         # tipado (null = ausente)
    assert linha[silver.COL_INGERIDO_EM] is not None
    assert silver.COL_PARAMS_JSON in rej.columns


def test_r7_transacao_sem_id_nulo_e_vazio(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, rej = _validar(spark, tmp_path, {
        "purchase": [
            _evento("purchase", "e-sem-id", params={"value": 10.0, "currency": "BRL"}),
            _evento("purchase", "e-id-vazio", params={"transaction_id": "  ", "value": 10.0, "currency": "BRL"}),
        ],
    })
    assert silver.MOTIVO_TRANSACAO_SEM_ID in _motivos_de(rej, "e-sem-id")
    assert silver.MOTIVO_TRANSACAO_SEM_ID in _motivos_de(rej, "e-id-vazio")


def test_r7_valor_monetario_invalido(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, rej = _validar(spark, tmp_path, {
        "view_item": [
            _evento("view_item", "e-negativo", params={"value": -5.0, "currency": "BRL"}),
            _evento("view_item", "e-usd", params={"value": 10.0, "currency": "USD"}),
            _evento("view_item", "e-usd-sem-value", params={"currency": "USD"}),
            _evento("view_item", "e-value-sem-currency", params={"value": 10.0}),
        ],
    })
    assert silver.MOTIVO_VALOR_INVALIDO in _motivos_de(rej, "e-negativo")
    assert silver.MOTIVO_VALOR_INVALIDO in _motivos_de(rej, "e-usd")
    # USD sem value -> não rejeita (cláusula só dispara com tem_value)
    assert validos.where(F.col("event_id") == "e-usd-sem-value").count() == 1
    # value sem currency -> não rejeita (ausência legítima de chave)
    assert validos.where(F.col("event_id") == "e-value-sem-currency").count() == 1


def test_r7_marco_fora_do_padrao_e_null_safe(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, rej = _validar(spark, tmp_path, {
        "scroll": [
            _evento("scroll", "e-33", params={"percent_scrolled": 33}),
            _evento("scroll", "e-50", params={"percent_scrolled": 50}),
            _evento("scroll", "e-sem-marco", params={}),
        ],
        "video_progress": [
            _evento("video_progress", "e-vid-fora", params={"video_percent": 33}),
        ],
    })
    assert silver.MOTIVO_MARCO_FORA in _motivos_de(rej, "e-33")
    assert silver.MOTIVO_MARCO_FORA in _motivos_de(rej, "e-vid-fora")
    # 50 é válido; scroll sem a chave de marco não vira marco_fora (null-safe)
    assert validos.where(F.col("event_id") == "e-50").count() == 1
    assert validos.where(F.col("event_id") == "e-sem-marco").count() == 1


def test_r7_valor_nao_numerico_acrescentado(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    pasta = tmp_path / "landing" / "event_name=view_item" / "year=2026" / "month=09" / "day=01"
    pasta.mkdir(parents=True, exist_ok=True)
    pasta.joinpath("coletor-s42-001.jsonl").write_text(
        '{"event_id":"e-abc","event_name":"view_item","event_ts":"2026-09-01T10:00:00.000Z",'
        '"user_pseudo_id":"anon-1","user_id":null,"session_hint":"s","page":{"location":"https://x/","referrer":null},'
        '"traffic":{"source":"google","medium":"cpc","campaign":"c"},'
        '"device":{"category":"mobile","os":"Android","browser":"Chrome"},'
        '"params":{"value":"abc","currency":"BRL"},"items":[]}\n',
        encoding="utf-8")
    bronze_df = bronze.construir_bronze(spark)
    tipado = silver.normalizar_pagina(silver.tipar_params(silver.deduplicar(bronze_df)))
    _, rej = silver.separar_validos_e_rejeitados(silver.marcar_refund_orfao(silver.classificar(tipado)))
    motivos = _motivos_de(rej, "e-abc")
    assert silver.MOTIVO_VALOR_NAO_NUMERICO in motivos
    assert silver.MOTIVO_VALOR_INVALIDO not in motivos  # distinto da regra mínima


def test_r7_marco_nao_numerico_acrescentado(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    pasta = tmp_path / "landing" / "event_name=scroll" / "year=2026" / "month=09" / "day=01"
    pasta.mkdir(parents=True, exist_ok=True)
    pasta.joinpath("coletor-s42-001.jsonl").write_text(
        '{"event_id":"e-marco-abc","event_name":"scroll","event_ts":"2026-09-01T10:00:00.000Z",'
        '"user_pseudo_id":"anon-1","user_id":null,"session_hint":"s","page":{"location":"https://x/","referrer":null},'
        '"traffic":{"source":"google","medium":"cpc","campaign":"c"},'
        '"device":{"category":"mobile","os":"Android","browser":"Chrome"},'
        '"params":{"percent_scrolled":"abc"},"items":[]}\n',
        encoding="utf-8")
    bronze_df = bronze.construir_bronze(spark)
    tipado = silver.normalizar_pagina(silver.tipar_params(silver.deduplicar(bronze_df)))
    _, rej = silver.separar_validos_e_rejeitados(silver.marcar_refund_orfao(silver.classificar(tipado)))
    motivos = _motivos_de(rej, "e-marco-abc")
    assert silver.MOTIVO_MARCO_NAO_NUMERICO in motivos
    assert silver.MOTIVO_MARCO_FORA not in motivos


def test_r7_refund_orfao_vs_com_compra(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, rej = _validar(spark, tmp_path, {
        "purchase": [_evento("purchase", "e-compra", params={"transaction_id": "T-1", "value": 10.0, "currency": "BRL"})],
        "refund": [
            _evento("refund", "e-refund-ok", params={"transaction_id": "T-1", "value": 10.0, "currency": "BRL"}),
            _evento("refund", "e-refund-orfao", params={"transaction_id": "T-999", "value": 10.0, "currency": "BRL"}),
        ],
    })
    assert validos.where(F.col("event_id") == "e-refund-ok").count() == 1
    assert silver.MOTIVO_REEMBOLSO_ORFAO in _motivos_de(rej, "e-refund-orfao")
    assert validos.where(F.col("event_id") == "e-compra").count() == 1


def test_r7_evento_viola_duas_regras_gera_duas_linhas(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    # purchase sem transaction_id E value negativo -> 2 motivos
    validos, rej = _validar(spark, tmp_path, {
        "purchase": [_evento("purchase", "e-dois", params={"value": -5.0, "currency": "BRL"})],
    })
    motivos = _motivos_de(rej, "e-dois")
    assert silver.MOTIVO_TRANSACAO_SEM_ID in motivos
    assert silver.MOTIVO_VALOR_INVALIDO in motivos
    assert rej.where(F.col("event_id") == "e-dois").count() == 2  # uma linha por motivo


def test_r7_aceitos_nao_rejeitam(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, rej = _validar(spark, tmp_path, {
        "scroll": [_evento("scroll", "e-ok", params={"percent_scrolled": 50})],
        # user_id nulo e element_id ausente não rejeitam
        "page_view": [_evento("page_view", "e-anon", params={"page_title": "home"})],
    })
    assert rej.count() == 0
    assert validos.count() == 2


# --------------------------------------------------------------------------- #
# `pagina` (5.4 item 12)
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("location,esperado", [
    ("https://loja.exemplo/", "/"),
    ("https://loja.exemplo/#planos", "/"),
    ("https://loja.exemplo/checkout?x=1", "/checkout"),
    ("https://loja.exemplo/obrigado#topo?a=b", "/obrigado"),
])
def test_pagina_normalizada(spark, tmp_path, monkeypatch, location, esperado):
    _apontar_para_local(monkeypatch, tmp_path)
    bronze_df = _bronze_de(spark, tmp_path, {
        "page_view": [_evento("page_view", "e1", params={"page_title": "x"}, location=location)],
    })
    df = silver.normalizar_pagina(silver.tipar_params(silver.deduplicar(bronze_df)))
    assert df.collect()[0].pagina == esperado


def test_pagina_location_nulo_vira_nulo(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    ev = _evento("page_view", "e1", params={"page_title": "x"})
    ev["page"] = {"location": None, "referrer": None}
    _escrever_landing(tmp_path, "page_view", "2026", "09", "01", [ev])
    df = silver.normalizar_pagina(silver.tipar_params(silver.deduplicar(bronze.construir_bronze(spark))))
    assert df.collect()[0].pagina is None


# --------------------------------------------------------------------------- #
# R8 — Sessionização
# --------------------------------------------------------------------------- #

def _sessionizar(spark, tmp_path, por_evento):
    bronze_df = _bronze_de(spark, tmp_path, por_evento)
    tipado = silver.normalizar_pagina(silver.tipar_params(silver.deduplicar(bronze_df)))
    validos, _ = silver.separar_validos_e_rejeitados(silver.marcar_refund_orfao(silver.classificar(tipado)))
    return silver.sessionizar(validos)


def test_r8_exemplo_do_enunciado_duas_sessoes(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    df = _sessionizar(spark, tmp_path, {
        "page_view": [
            _evento("page_view", "e1", event_ts=_ts("10:00"), params={"page_title": "a"}),
            _evento("page_view", "e3", event_ts=_ts("10:50"), params={"page_title": "a"}),
        ],
        "scroll": [_evento("scroll", "e2", event_ts=_ts("10:05"), params={"percent_scrolled": 25})],
    })
    sessoes = {l.session_id for l in df.collect()}
    assert len(sessoes) == 2


def test_r8_gap_exatamente_30min_mesma_sessao(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    df = _sessionizar(spark, tmp_path, {
        "page_view": [
            _evento("page_view", "e1", event_ts=_ts("10:00"), params={"page_title": "a"}),
            _evento("page_view", "e2", event_ts=_ts("10:30"), params={"page_title": "a"}),
        ],
    })
    assert len({l.session_id for l in df.collect()}) == 1  # 30 min exatos não corta


def test_r8_ordem_estavel_timestamps_iguais(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    df = _sessionizar(spark, tmp_path, {
        "page_view": [
            _evento("page_view", "e2", event_ts=_ts("10:00"), params={"page_title": "a"}),
            _evento("page_view", "e1", event_ts=_ts("10:00"), params={"page_title": "a"}),
        ],
    })
    # mesma sessão, ordinal 1 para ambos; resultado determinístico
    assert len({l.session_id for l in df.collect()}) == 1


def test_r8_independe_de_session_hint(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    # session_hint diferente em cada evento, mas gap < 30 min -> 1 sessão
    df = _sessionizar(spark, tmp_path, {
        "page_view": [
            _evento("page_view", "e1", event_ts=_ts("10:00"), session_hint="h-1", params={"page_title": "a"}),
            _evento("page_view", "e2", event_ts=_ts("10:10"), session_hint="h-2", params={"page_title": "a"}),
        ],
    })
    assert len({l.session_id for l in df.collect()}) == 1


def test_r8_usuarios_distintos_nao_se_misturam(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    df = _sessionizar(spark, tmp_path, {
        "page_view": [
            _evento("page_view", "e1", user_pseudo_id="anon-1", event_ts=_ts("10:00"), params={"page_title": "a"}),
            _evento("page_view", "e2", user_pseudo_id="anon-2", event_ts=_ts("10:05"), params={"page_title": "a"}),
        ],
    })
    por_id = {l.event_id: l.session_id for l in df.collect()}
    assert por_id["e1"] != por_id["e2"]
    assert por_id["e1"].startswith("anon-1#")
    assert por_id["e2"].startswith("anon-2#")


def test_r8_formato_session_id(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    df = _sessionizar(spark, tmp_path, {
        "page_view": [_evento("page_view", "e1", user_pseudo_id="anon-x", event_ts=_ts("10:00"), params={"page_title": "a"})],
    })
    assert df.collect()[0].session_id == "anon-x#1"


# --------------------------------------------------------------------------- #
# R9 — Identity stitching
# --------------------------------------------------------------------------- #

def _silver_completo(spark, tmp_path, por_evento):
    bronze_df = _bronze_de(spark, tmp_path, por_evento)
    dedup = silver.deduplicar(bronze_df)
    tipado = silver.normalizar_pagina(silver.tipar_params(dedup))
    validos, rej = silver.separar_validos_e_rejeitados(silver.marcar_refund_orfao(silver.classificar(tipado)))
    validos = silver.sessionizar(validos)
    validos = silver.costurar_identidade(validos, tipado)
    return validos, rej, tipado


def test_r9_eventos_anteriores_ao_login_recebem_user_id(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, _, _ = _silver_completo(spark, tmp_path, {
        "page_view": [_evento("page_view", "e-antes", user_pseudo_id="anon-1", event_ts=_ts("10:00"),
                              user_id=None, params={"page_title": "a"})],
        "login": [_evento("login", "e-login", user_pseudo_id="anon-1", event_ts=_ts("10:10"),
                          user_id="u-123", params={"method": "email"})],
    })
    por_id = {l.event_id: l for l in validos.collect()}
    assert por_id["e-antes"].user_id_resolvido == "u-123"
    assert por_id["e-antes"].user_id is None  # original preservado


def test_r9_precedencia_primeiro_user_id_no_tempo(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, _, _ = _silver_completo(spark, tmp_path, {
        "login": [
            _evento("login", "e-login1", user_pseudo_id="anon-1", event_ts=_ts("10:00"),
                    user_id="u-primeiro", params={"method": "email"}),
            _evento("login", "e-login2", user_pseudo_id="anon-1", event_ts=_ts("11:00"),
                    user_id="u-segundo", params={"method": "email"}),
        ],
    })
    for l in validos.collect():
        assert l.user_id_resolvido == "u-primeiro"


def test_r9_usuarios_distintos_nao_compartilham(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, _, _ = _silver_completo(spark, tmp_path, {
        "login": [
            _evento("login", "e1", user_pseudo_id="anon-1", event_ts=_ts("10:00"), user_id="u-1", params={"method": "email"}),
            _evento("login", "e2", user_pseudo_id="anon-2", event_ts=_ts("10:00"), user_id="u-2", params={"method": "email"}),
        ],
    })
    por_id = {l.event_id: l.user_id_resolvido for l in validos.collect()}
    assert por_id["e1"] == "u-1"
    assert por_id["e2"] == "u-2"


def test_r9_user_id_resolvido_nulo_quando_nunca_logou(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, _, _ = _silver_completo(spark, tmp_path, {
        "page_view": [_evento("page_view", "e1", user_pseudo_id="anon-1", event_ts=_ts("10:00"),
                              user_id=None, params={"page_title": "a"})],
    })
    assert validos.collect()[0].user_id_resolvido is None


def test_r9_login_rejeitado_ainda_costura(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    # login com ts futuro -> rejeitado, mas ainda contribui ao mapa
    validos, rej, _ = _silver_completo(spark, tmp_path, {
        "page_view": [_evento("page_view", "e-antes", user_pseudo_id="anon-1", event_ts=_ts("10:00"),
                              user_id=None, params={"page_title": "a"})],
        "login": [_evento("login", "e-login-fut", user_pseudo_id="anon-1", event_ts="2099-01-01T00:00:00.000Z",
                          user_id="u-rej", params={"method": "email"})],
    })
    # o login foi rejeitado
    assert rej.where(F.col("event_id") == "e-login-fut").count() == 1
    # mas o evento válido anterior recebeu o user_id do login rejeitado
    assert validos.where(F.col("event_id") == "e-antes").collect()[0].user_id_resolvido == "u-rej"


def test_r9_join_1para1_nao_multiplica(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    validos, _, _ = _silver_completo(spark, tmp_path, {
        "page_view": [
            _evento("page_view", "e1", user_pseudo_id="anon-1", event_ts=_ts("10:00"), user_id=None, params={"page_title": "a"}),
            _evento("page_view", "e2", user_pseudo_id="anon-1", event_ts=_ts("10:05"), user_id=None, params={"page_title": "a"}),
        ],
        "login": [
            _evento("login", "e3", user_pseudo_id="anon-1", event_ts=_ts("10:02"), user_id="u-1", params={"method": "email"}),
            _evento("login", "e4", user_pseudo_id="anon-1", event_ts=_ts("10:03"), user_id="u-1", params={"method": "email"}),
        ],
    })
    assert validos.count() == 4  # não multiplicou


# --------------------------------------------------------------------------- #
# R10 — Itens
# --------------------------------------------------------------------------- #

def _itens(spark, tmp_path, por_evento):
    validos, _, _ = _silver_completo(spark, tmp_path, por_evento)
    return silver.explodir_itens(validos), validos


def test_r10_purchase_com_dois_itens_gera_duas_linhas(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    itens, _ = _itens(spark, tmp_path, {
        "purchase": [_evento("purchase", "e-compra",
                            params={"transaction_id": "T-1", "value": 744.0, "currency": "BRL"},
                            items=[
                                {"item_id": "PLANO-PRO", "item_name": "Plano Pro", "item_category": "plano",
                                 "price": 597.0, "quantity": 1},
                                {"item_id": "BUMP-MENTORIA", "item_name": "Mentoria", "item_category": "bump",
                                 "price": 147.0, "quantity": 1},
                            ])],
    })
    linhas = itens.where(F.col("event_id") == "e-compra").collect()
    assert len(linhas) == 2
    assert {l.item_id for l in linhas} == {"PLANO-PRO", "BUMP-MENTORIA"}


def test_r10_items_vazio_ou_nulo_gera_zero_linhas(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    itens, _ = _itens(spark, tmp_path, {
        "page_view": [_evento("page_view", "e-sem-itens", params={"page_title": "a"}, items=[])],
    })
    assert itens.count() == 0


def test_r10_integridade_referencial(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    itens, validos = _itens(spark, tmp_path, {
        "purchase": [_evento("purchase", "e-compra",
                            params={"transaction_id": "T-1", "value": 597.0, "currency": "BRL"},
                            items=[{"item_id": "PLANO-PRO", "item_name": "Plano Pro", "item_category": "plano",
                                    "price": 597.0, "quantity": 1}])],
    })
    orfaos = itens.select("event_id").distinct().subtract(validos.select("event_id")).count()
    assert orfaos == 0


def test_r10_schema_de_saida(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    itens, _ = _itens(spark, tmp_path, {
        "purchase": [_evento("purchase", "e-compra",
                            params={"transaction_id": "T-1", "value": 597.0, "currency": "BRL"},
                            items=[{"item_id": "PLANO-PRO", "item_name": "Plano Pro", "item_category": "plano",
                                    "price": 597.0, "quantity": 1, "index": 0,
                                    "item_list_id": "planos", "item_list_name": "Planos"}])],
    })
    tipos = dict(itens.dtypes)
    assert tipos["item_id"] == "string"
    assert tipos["item_category"] == "string"
    assert tipos["price"] == "double"
    assert tipos["quantity"] == "bigint"
    assert tipos["event_id"] == "string"
    assert tipos["session_id"] == "string"
    assert tipos["user_id_resolvido"] == "string"
    assert tipos["event_ts_ts"] == "timestamp"
    for p in ("year", "month", "day"):
        assert tipos[p] == "string"


def test_r10_purchase_sem_item_plano_nao_rejeita(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    itens, validos = _itens(spark, tmp_path, {
        "purchase": [_evento("purchase", "e-compra",
                            params={"transaction_id": "T-1", "value": 147.0, "currency": "BRL"},
                            items=[{"item_id": "BUMP-MENTORIA", "item_name": "Mentoria", "item_category": "bump",
                                    "price": 147.0, "quantity": 1}])],
    })
    # o purchase sem item de categoria 'plano' não é rejeitado
    assert validos.where(F.col("event_id") == "e-compra").count() == 1
    linhas = itens.where(F.col("event_id") == "e-compra").collect()
    assert all(l.item_category != "plano" for l in linhas)


def test_r10_invariante_contagem(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    itens, validos = _itens(spark, tmp_path, {
        "purchase": [_evento("purchase", "e-compra",
                            params={"transaction_id": "T-1", "value": 744.0, "currency": "BRL"},
                            items=[
                                {"item_id": "PLANO-PRO", "item_name": "Plano Pro", "item_category": "plano", "price": 597.0, "quantity": 1},
                                {"item_id": "BUMP-MENTORIA", "item_name": "Mentoria", "item_category": "bump", "price": 147.0, "quantity": 1},
                            ])],
        "page_view": [_evento("page_view", "e-pv", params={"page_title": "a"}, items=[])],
    })
    soma = (validos.where(F.size(F.coalesce(F.col("items"), F.array())) > 0)
            .select(F.explode("items")).count())
    assert itens.count() == soma == 2


def test_r10_particao_dia_do_evento_utc(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    # evento coletado no dia 01 (partição da landing) mas com event_ts no dia 02 UTC
    _escrever_landing(tmp_path, "purchase", "2026", "09", "01", [
        _evento("purchase", "e-vira-dia", event_ts="2026-09-02T00:30:00.000Z",
                params={"transaction_id": "T-1", "value": 597.0, "currency": "BRL"},
                items=[{"item_id": "PLANO-PRO", "item_name": "Plano Pro", "item_category": "plano",
                        "price": 597.0, "quantity": 1}]),
    ])
    bronze_df = bronze.construir_bronze(spark)
    dedup = silver.deduplicar(bronze_df)
    tipado = silver.normalizar_pagina(silver.tipar_params(dedup))
    v, _ = silver.separar_validos_e_rejeitados(silver.marcar_refund_orfao(silver.classificar(tipado)))
    v = silver.costurar_identidade(silver.sessionizar(v), tipado)
    itens = silver.explodir_itens(v)
    linha = itens.collect()[0]
    assert (linha.year, linha.month, linha.day) == ("2026", "09", "02")


# --------------------------------------------------------------------------- #
# Integração: gravação / releitura / preservação / idempotência / reconciliação
# --------------------------------------------------------------------------- #

def _bronze_mista(spark, tmp_path):
    """Dataset pequeno com válidos, rejeitados, duplicata e itens."""
    _escrever_landing(tmp_path, "page_view", "2026", "09", "01", [
        _evento("page_view", "v1", user_pseudo_id="anon-1", event_ts=_ts("10:00"), params={"page_title": "a"}),
    ])
    _escrever_landing(tmp_path, "page_view", "2026", "09", "01", [
        _evento("page_view", "v1", user_pseudo_id="anon-1", event_ts=_ts("10:00"), params={"page_title": "a"}),
    ], arquivo="coletor-s42-002.jsonl")  # duplicata de v1
    _escrever_landing(tmp_path, "scroll", "2026", "09", "01", [
        _evento("scroll", "r1", event_ts=_ts("10:01"), params={"percent_scrolled": 33}),  # rejeitado marco
    ])
    _escrever_landing(tmp_path, "purchase", "2026", "09", "01", [
        _evento("purchase", "p1", user_pseudo_id="anon-1", event_ts=_ts("10:05"),
                params={"transaction_id": "T-1", "value": 597.0, "currency": "BRL"},
                items=[{"item_id": "PLANO-PRO", "item_name": "Plano Pro", "item_category": "plano",
                        "price": 597.0, "quantity": 1}]),
    ])
    return bronze.construir_bronze(spark)


def test_integracao_gravacao_releitura_e_preservacao(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    bronze_df = _bronze_mista(spark, tmp_path)
    dedup = silver.deduplicar(bronze_df)
    tipado = silver.normalizar_pagina(silver.tipar_params(dedup))
    validos, rej = silver.separar_validos_e_rejeitados(silver.marcar_refund_orfao(silver.classificar(tipado)))
    validos = silver.costurar_identidade(silver.sessionizar(validos), tipado)
    itens = silver.explodir_itens(validos)

    silver._gravar(validos, rej, itens)

    # os três prefixos existem e são relíveis
    val_lidos = comum.ler_eventos(spark, "silver")
    itens_lidos = spark.read.parquet(comum.caminho("silver/itens"))
    rej_lidos = spark.read.parquet(comum.caminho("silver/eventos_rejeitados"))
    assert val_lidos.count() == validos.count()
    assert itens_lidos.count() == itens.count()
    assert rej_lidos.count() == rej.count()
    # month/day texto de 2 dígitos
    assert val_lidos.collect()[0].month == "09"
    # gravar itens/rejeitados não apagou silver/event_name=
    assert comum.existe(spark, comum.caminho(comum.glob_eventos("silver"))) is True


def test_idempotencia_duas_execucoes_nao_acumulam(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    bronze_df = _bronze_mista(spark, tmp_path)

    def rodar():
        dedup = silver.deduplicar(bronze.construir_bronze(spark))
        tipado = silver.normalizar_pagina(silver.tipar_params(dedup))
        validos, rej = silver.separar_validos_e_rejeitados(silver.marcar_refund_orfao(silver.classificar(tipado)))
        validos = silver.costurar_identidade(silver.sessionizar(validos), tipado)
        itens = silver.explodir_itens(validos)
        silver._gravar(validos, rej, itens)
        return (comum.ler_eventos(spark, "silver").count(),
                spark.read.parquet(comum.caminho("silver/itens")).count(),
                spark.read.parquet(comum.caminho("silver/eventos_rejeitados")).count())

    primeira = rodar()
    segunda = rodar()
    assert primeira == segunda


def test_reconciliacao_fecha(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    bronze_df = _bronze_mista(spark, tmp_path)
    dedup = silver.deduplicar(bronze_df)
    tipado = silver.normalizar_pagina(silver.tipar_params(dedup))
    validos, rej = silver.separar_validos_e_rejeitados(silver.marcar_refund_orfao(silver.classificar(tipado)))

    processados = bronze_df.count()
    eventos_unicos = dedup.count()
    duplicatas = processados - eventos_unicos
    n_validos = validos.count()
    rej_distintos = silver._contar_rejeitados_distintos(rej)

    assert processados == n_validos + duplicatas + rej_distintos
    assert n_validos + rej_distintos == eventos_unicos
    # disjunção
    intersec = validos.select("event_id").intersect(rej.select("event_id")).count()
    assert intersec == 0


def test_reconciliacao_event_id_nulo_conta_como_um(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    # um rejeitado sem event_id (fora do catálogo), contado como 1 evento
    _escrever_landing(tmp_path, "add_to_wishlist", "2026", "09", "01", [
        _evento("add_to_wishlist", None, params={}),
    ])
    bronze_df = bronze.construir_bronze(spark)
    dedup = silver.deduplicar(bronze_df)
    tipado = silver.normalizar_pagina(silver.tipar_params(dedup))
    _, rej = silver.separar_validos_e_rejeitados(silver.marcar_refund_orfao(silver.classificar(tipado)))
    assert silver._contar_rejeitados_distintos(rej) == 1


# --------------------------------------------------------------------------- #
# exigir_camada (R2)
# --------------------------------------------------------------------------- #

def test_exigir_camada_bronze_ausente(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    monkeypatch.setattr(spark, "stop", lambda: None)  # sessão compartilhada
    with pytest.raises(SystemExit) as erro:
        silver.construir_silver(spark)
    assert "Rode antes: python pipeline/bronze.py" in str(erro.value)
