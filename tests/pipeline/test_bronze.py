"""Testes do job Bronze (pipeline/bronze.py).

Seguem o padrão de tests/pipeline/test_comum.py: usam a fixture `spark` (local),
e trocam comum.caminho por um caminho file:// numa pasta temporária, para
exercitar a lógica real de leitura/gravação sem depender do floci/S3.

O que cobrem (requisitos R1/R2/R4):
- leitura de eventos válidos da landing com schema explícito;
- preservação dos campos, dos tipos e da estrutura aninhada do contrato;
- registro das colunas de proveniência;
- construção dos caminhos e particionamento event_name/year/month/day;
- gravação correta em Parquet;
- origem inexistente/vazia (exigir_camada);
- dados incompatíveis com o contrato (colunas extras ignoradas, tipos coagidos
  pelo schema; nada de inferência);
- semântica append-only: execuções adicionais não apagam o que já existe.
"""

from __future__ import annotations

import json

import pytest

import bronze
import comum
from pyspark.sql.types import ArrayType, MapType, StringType, StructType


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _apontar_para_local(monkeypatch, tmp_path):
    """Faz comum.caminho (e o usado por bronze) apontar para uma pasta local."""
    monkeypatch.setattr(comum, "caminho", lambda camada: f"file://{tmp_path}/{camada}")


def _escrever_landing(tmp_path, event_name, year, month, day, eventos, arquivo="coletor-s42-001.jsonl"):
    """Grava um arquivo JSONL na partição da landing, como o coletor faria."""
    pasta = tmp_path / "landing" / f"event_name={event_name}" / f"year={year}" / f"month={month}" / f"day={day}"
    pasta.mkdir(parents=True, exist_ok=True)
    linhas = "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in eventos)
    (pasta / arquivo).write_text(linhas, encoding="utf-8")


def _evento_valido(**sobrescrever):
    """Um evento purchase completo, nos moldes do docs/CONTRATO_EVENTO.md."""
    base = {
        "event_id": "69e0446a-5000-4c95-9a90-0195c8f66be1",
        "event_name": "purchase",
        "event_ts": "2026-09-01T14:22:05.123Z",
        "user_pseudo_id": "anon-9a46b593",
        "user_id": None,
        "session_hint": "s-289b523a",
        "page": {"location": "https://loja.exemplo/checkout", "referrer": "https://loja.exemplo/"},
        "traffic": {"source": "google", "medium": "cpc", "campaign": "formacao_dados"},
        "device": {"category": "mobile", "os": "Android", "browser": "Chrome"},
        "params": {"transaction_id": "T-5BA58C26", "value": 1086.8, "currency": "BRL", "payment_type": "pix"},
        "items": [{"item_id": "PLANO-PREMIUM", "item_name": "Plano Premium",
                   "item_category": "plano", "price": 997.0, "quantity": 1}],
    }
    base.update(sobrescrever)
    return base


# --------------------------------------------------------------------------- #
# Schema explícito (R4)
# --------------------------------------------------------------------------- #

def test_schema_tem_os_campos_do_contrato_sem_event_name():
    nomes = [c.name for c in bronze.SCHEMA.fields]
    # campos simples + objetos aninhados + params + items.
    # event_name NÃO entra no schema de leitura: vem da partição do caminho,
    # com o mesmo valor do corpo do evento (evita a coluna duplicada).
    assert nomes == [
        "event_id", "event_ts", "user_pseudo_id", "user_id", "session_hint",
        "page", "traffic", "device", "params", "items",
    ]
    assert "event_name" not in nomes


def test_schema_preserva_estrutura_aninhada_e_tipos():
    campos = {c.name: c.dataType for c in bronze.SCHEMA.fields}
    assert isinstance(campos["page"], StructType)
    assert isinstance(campos["traffic"], StructType)
    assert isinstance(campos["device"], StructType)
    # params é lido como STRING (o JSON cru, fiel); o mapa consultável é derivado
    # dele em construir_bronze (ver test_params_json_cru_e_map_derivado).
    assert isinstance(campos["params"], StringType)
    assert isinstance(campos["items"], ArrayType)         # lista de itens
    assert {f.name for f in campos["page"].fields} == {"location", "referrer"}
    assert {f.name for f in campos["traffic"].fields} == {"source", "medium", "campaign"}
    assert {f.name for f in campos["device"].fields} == {"category", "os", "browser"}


def test_event_ts_fica_como_texto_na_bronze():
    """A Bronze é cópia fiel; a tipagem de event_ts é da Silver."""
    campos = {c.name: c.dataType.simpleString() for c in bronze.SCHEMA.fields}
    assert campos["event_ts"] == "string"


# --------------------------------------------------------------------------- #
# Leitura de eventos válidos + proveniência (R1/R4)
# --------------------------------------------------------------------------- #

def test_le_eventos_validos_e_registra_proveniencia(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    _escrever_landing(tmp_path, "purchase", "2026", "09", "01", [_evento_valido()])

    bronze_df = bronze.construir_bronze(spark)
    linhas = bronze_df.collect()
    assert len(linhas) == 1
    linha = linhas[0]

    # proveniência presente e preenchida (R4)
    assert bronze.COL_ARQUIVO_ORIGEM in bronze_df.columns
    assert bronze.COL_INGERIDO_EM in bronze_df.columns
    assert linha[bronze.COL_ARQUIVO_ORIGEM].endswith("coletor-s42-001.jsonl")
    assert linha[bronze.COL_INGERIDO_EM] is not None

    # colunas de partição vindas do caminho
    assert (linha.event_name, linha.year, linha.month, linha.day) == ("purchase", "2026", "09", "01")


def test_preserva_campos_tipos_e_estrutura_aninhada(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    _escrever_landing(tmp_path, "purchase", "2026", "09", "01", [_evento_valido()])

    linha = bronze.construir_bronze(spark).collect()[0]
    assert linha.event_id == "69e0446a-5000-4c95-9a90-0195c8f66be1"
    assert linha.event_ts == "2026-09-01T14:22:05.123Z"
    assert linha.user_id is None
    assert linha.page.location == "https://loja.exemplo/checkout"
    assert linha.traffic.source == "google"
    assert linha.device.category == "mobile"
    # params (map derivado): chaves consultáveis como texto
    assert linha.params["transaction_id"] == "T-5BA58C26"
    assert linha.params["currency"] == "BRL"
    # items preservado como lista de struct
    assert linha["items"][0].item_id == "PLANO-PREMIUM"
    assert linha["items"][0].price == 997.0
    assert linha["items"][0].quantity == 1


def test_params_json_cru_e_map_derivado(spark, tmp_path, monkeypatch):
    """A Bronze guarda params de duas formas, sem perder nada:
    params_json (objeto JSON cru, fiel) e params (map<string,string> derivado)."""
    _apontar_para_local(monkeypatch, tmp_path)
    _escrever_landing(tmp_path, "purchase", "2026", "09", "01", [
        _evento_valido(event_id="e-tipos", params={
            "value": 1086.8,           # número decimal
            "search_results": 0,       # número inteiro
            "outbound": True,          # booleano
            "currency": "BRL",         # texto
            "coupon": None,            # nulo
        }),
    ])
    bronze_df = bronze.construir_bronze(spark)

    assert bronze.COL_PARAMS_JSON in bronze_df.columns      # params_json
    assert bronze.COL_PARAMS in bronze_df.columns           # params (map)
    # params (map): é um map<string,string>
    tipos = {c.name: c.dataType for c in bronze_df.schema.fields}
    assert isinstance(tipos[bronze.COL_PARAMS], MapType)
    assert isinstance(tipos[bronze.COL_PARAMS_JSON], StringType)

    linha = bronze_df.collect()[0]
    # params_json preserva os TIPOS JSON originais. Comparamos a ESTRUTURA (json.loads),
    # não o texto: o espaçamento do JSON depende do serializador, não é fidelidade.
    bruto = json.loads(linha[bronze.COL_PARAMS_JSON])
    assert bruto["value"] == 1086.8                 # número decimal (não texto)
    assert bruto["search_results"] == 0             # número inteiro
    assert bruto["outbound"] is True                # booleano (não "true")
    assert bruto["currency"] == "BRL"               # texto
    assert "coupon" in bruto and bruto["coupon"] is None   # nulo preservado
    # params (map): mesmas chaves, valores como texto; a chave nula sobrevive
    m = linha[bronze.COL_PARAMS]
    assert m["value"] == "1086.8"
    assert m["outbound"] == "true"
    assert "coupon" in m and m["coupon"] is None


def test_params_json_preserva_aninhado_inesperado(spark, tmp_path, monkeypatch):
    """params inesperado com objeto/array aninhado não é descartado: o JSON cru
    fica intacto em params_json e o map guarda o valor como texto JSON."""
    _apontar_para_local(monkeypatch, tmp_path)
    _escrever_landing(tmp_path, "click", "2026", "09", "01", [
        _evento_valido(event_id="e-aninhado", event_name="click", items=[], params={
            "inesperado": {"a": 1, "b": [1, 2, 3]},
            "lista": [True, None, "x"],
        }),
    ])
    linha = bronze.construir_bronze(spark).collect()[0]
    # params_json: o aninhado é preservado com tipos (comparação por estrutura)
    bruto = json.loads(linha[bronze.COL_PARAMS_JSON])
    assert bruto["inesperado"] == {"a": 1, "b": [1, 2, 3]}
    assert bruto["lista"] == [True, None, "x"]
    # no map, o valor aninhado vira o texto JSON do valor (nada perdido)
    m = linha[bronze.COL_PARAMS]
    assert json.loads(m["inesperado"]) == {"a": 1, "b": [1, 2, 3]}
    assert json.loads(m["lista"]) == [True, None, "x"]


def test_params_ausente_vira_nulo_sem_quebrar(spark, tmp_path, monkeypatch):
    """Evento sem params: params_json nulo e map nulo, sem erro (cópia fiel)."""
    _apontar_para_local(monkeypatch, tmp_path)
    evento = _evento_valido(event_id="e-sem-params")
    del evento["params"]
    _escrever_landing(tmp_path, "purchase", "2026", "09", "01", [evento])
    linha = bronze.construir_bronze(spark).collect()[0]
    assert linha[bronze.COL_PARAMS_JSON] is None
    assert linha[bronze.COL_PARAMS] is None


# --------------------------------------------------------------------------- #
# Caminhos e particionamento (R4)
# --------------------------------------------------------------------------- #

def test_gravacao_particiona_por_event_name_year_month_day(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    _escrever_landing(tmp_path, "scroll", "2026", "09", "03", [
        _evento_valido(event_name="scroll", event_id="e-scroll",
                       params={"percent_scrolled": 50}, items=[]),
    ])
    _escrever_landing(tmp_path, "purchase", "2026", "09", "01", [_evento_valido(event_id="e-purchase")])

    bronze_df = bronze.construir_bronze(spark)
    comum.gravar_eventos(bronze_df, "bronze", "append")

    # a árvore de partições existe no disco, com month/day de dois dígitos
    assert (tmp_path / "bronze" / "event_name=scroll" / "year=2026" / "month=09" / "day=03").is_dir()
    assert (tmp_path / "bronze" / "event_name=purchase" / "year=2026" / "month=09" / "day=01").is_dir()


def test_grava_parquet_legivel_de_volta(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    _escrever_landing(tmp_path, "purchase", "2026", "09", "01", [_evento_valido()])

    comum.gravar_eventos(bronze.construir_bronze(spark), "bronze", "append")

    relido = comum.ler_eventos(spark, "bronze")
    assert relido.count() == 1
    linha = relido.collect()[0]
    assert linha.event_id == "69e0446a-5000-4c95-9a90-0195c8f66be1"
    assert linha.month == "09"                         # texto, não 9
    assert bronze.COL_ARQUIVO_ORIGEM in relido.columns
    assert bronze.COL_INGERIDO_EM in relido.columns
    # as duas colunas de params sobrevivem ao round-trip Parquet
    assert bronze.COL_PARAMS_JSON in relido.columns
    assert bronze.COL_PARAMS in relido.columns
    assert json.loads(linha[bronze.COL_PARAMS_JSON])["transaction_id"] == "T-5BA58C26"
    assert linha[bronze.COL_PARAMS]["currency"] == "BRL"


# --------------------------------------------------------------------------- #
# Origem inexistente ou vazia (R2)
# --------------------------------------------------------------------------- #

def test_origem_inexistente_para_com_mensagem(spark, tmp_path, monkeypatch):
    _apontar_para_local(monkeypatch, tmp_path)
    monkeypatch.setattr(spark, "stop", lambda: None)   # a sessão é compartilhada
    with pytest.raises(SystemExit) as erro:
        bronze.construir_bronze(spark)
    assert "Rode antes: python gerador/gerar_eventos.py" in str(erro.value)


# --------------------------------------------------------------------------- #
# Dados incompatíveis com o contrato
# --------------------------------------------------------------------------- #

def test_colunas_extras_sao_ignoradas_pelo_schema_explicito(spark, tmp_path, monkeypatch):
    """Schema explícito (sem inferência): um campo fora do contrato não entra."""
    _apontar_para_local(monkeypatch, tmp_path)
    evento = _evento_valido()
    evento["campo_desconhecido"] = {"qualquer": 1}
    _escrever_landing(tmp_path, "purchase", "2026", "09", "01", [evento])

    bronze_df = bronze.construir_bronze(spark)
    assert "campo_desconhecido" not in bronze_df.columns
    assert bronze_df.count() == 1


def test_evento_sujo_e_ingerido_sem_rejeicao(spark, tmp_path, monkeypatch):
    """A Bronze é cópia fiel: um evento com defeito (ts nulo, value negativo,
    event_name fora do catálogo) ENTRA na Bronze. A rejeição é da Silver."""
    _apontar_para_local(monkeypatch, tmp_path)
    _escrever_landing(tmp_path, "purchase", "2026", "09", "01", [
        _evento_valido(event_id="e-ts-nulo", event_ts=None),
    ])
    _escrever_landing(tmp_path, "add_to_wishlist", "2026", "09", "01", [
        _evento_valido(event_id="e-fora", event_name="add_to_wishlist", params={}, items=[]),
    ])

    bronze_df = bronze.construir_bronze(spark)
    ids = {l.event_id for l in bronze_df.collect()}
    assert ids == {"e-ts-nulo", "e-fora"}
    # o evento fora do catálogo foi preservado, com a partição que o coletor deu
    fora = [l for l in bronze_df.collect() if l.event_id == "e-fora"][0]
    assert fora.event_name == "add_to_wishlist"


# --------------------------------------------------------------------------- #
# Append-only (R4)
# --------------------------------------------------------------------------- #

def test_append_preserva_execucoes_anteriores(spark, tmp_path, monkeypatch):
    """Rodar a Bronze de novo acrescenta, não apaga o histórico (append-only)."""
    _apontar_para_local(monkeypatch, tmp_path)

    _escrever_landing(tmp_path, "purchase", "2026", "09", "01", [_evento_valido(event_id="e1")])
    comum.gravar_eventos(bronze.construir_bronze(spark), "bronze", "append")
    assert comum.ler_eventos(spark, "bronze").count() == 1

    # segunda carga: outro dia, outro lote (como um coletor que descarrega de novo)
    _escrever_landing(tmp_path, "purchase", "2026", "09", "02",
                      [_evento_valido(event_id="e2", event_ts="2026-09-02T10:00:00.000Z")],
                      arquivo="coletor-s42-002.jsonl")
    comum.gravar_eventos(bronze.construir_bronze(spark), "bronze", "append")

    relido = comum.ler_eventos(spark, "bronze")
    ids = sorted(l.event_id for l in relido.collect())
    # e1 continua lá (append-only) e e2 foi acrescentado; e1 aparece 2x porque a
    # landing ainda o contém nas duas leituras — a dedup é da Silver (R5), não aqui.
    assert ids.count("e1") == 2
    assert ids.count("e2") == 1
    assert (tmp_path / "bronze" / "event_name=purchase" / "year=2026" / "month=09" / "day=01").is_dir()
    assert (tmp_path / "bronze" / "event_name=purchase" / "year=2026" / "month=09" / "day=02").is_dir()
