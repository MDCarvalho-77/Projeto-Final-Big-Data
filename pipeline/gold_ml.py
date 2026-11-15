"""Job Gold — machine learning (design §6; FEAT-004).

Lê a Silver (eventos válidos) e grava quatro tabelas analíticas em ``gold/``:

- ``gold/propensao_sessao``   — propensão de compra por sessão (todas as sessões).
- ``gold/metricas_modelo``    — métricas reais do modelo (EXATAMENTE 4 linhas).
- ``gold/segmentos_visitantes`` — segmento (cluster) de cada visitante.
- ``gold/segmentos_perfil``   — perfil médio de cada segmento (par de #13).

A espinha (design §6): construir uma tabela de features por sessão **cortada no
primeiro ``add_to_cart``** (sem vazamento, §6.1), treinar uma regressão logística,
escorar **todas** as sessões, e clusterizar os visitantes por K-Means.

Anti-vazamento (§6.1, dupla barreira):
- corte temporal: só eventos com ``event_ts_ts < t_corte``, onde ``t_corte`` é o
  menor ``event_ts_ts`` de um ``add_to_cart`` da sessão (``null`` se não houver);
- lista negra: ``view_cart, begin_checkout, add_shipping_info, add_payment_info,
  purchase, refund, add_to_cart`` nunca entram nas features.

Reaproveita ``pipeline/comum.py`` (SparkSession, caminhos s3a://, verificação de
camada) e ``pipeline/gold_comum.py`` (``atributos_sessao`` — canal e dispositivo
da sessão, design §1.2). Acesso ao S3 só por Spark; gravação Parquet, ``overwrite``
por tabela e **sem** partição. Spark ML (``pyspark.ml``), sem dependência nova.
Modo ANSI do Spark 4 respeitado. Idioma: português.
"""

from __future__ import annotations

import comum
from comum import criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo
from gold_comum import atributos_sessao

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    ClusteringEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)

# --------------------------------------------------------------------------- #
# Constantes de módulo
# --------------------------------------------------------------------------- #

# Semente fixa para reprodutibilidade do split e do K-Means (design §6.3).
SEED = 42

# Lista negra de eventos de checkout — nunca entram nas features (§6.1). O
# add_to_cart é a fronteira (também excluído). Qualquer evento do checkout
# "praticamente revela a compra", então é barrado mesmo em sessões sem add_to_cart.
EVENTOS_CHECKOUT = [
    "add_to_cart",
    "view_cart",
    "begin_checkout",
    "add_shipping_info",
    "add_payment_info",
    "purchase",
    "refund",
]

# Features numéricas/booleanas por sessão (só pré-corte, §6.2). A ordem é fixa e
# registrada em metricas_modelo.features.
FEATURES_NUMERICAS = [
    "scroll_max",
    "video_percent_max",
    "n_eventos_pre",
    "n_view_item_list",
    "n_select_item",
    "n_view_item",
    "n_site_search",
    "n_file_download",
    "n_click",
    "fez_lead_pre",
    "tem_select_item",
]

# Features categóricas da sessão (§6.2).
FEATURES_CATEGORICAS = ["canal", "dispositivo"]

# Lista exata de features (string registrada em metricas_modelo.features).
LISTA_FEATURES = ", ".join(FEATURES_NUMERICAS + FEATURES_CATEGORICAS)

# K-Means: faixa de K varrida por silhueta (design §6.4).
K_MIN = 2
K_MAX = 5

# Features do visitante para o K-Means (agregadas das sessões), §6.4.
FEATURES_VISITANTE = [
    "n_sessoes",
    "scroll_medio",
    "video_medio",
    "comprou_alguma_vez",
    "n_eventos_medio",
]


def _gravar(df: DataFrame, tabela: str) -> None:
    """Grava uma tabela Gold em ``gold/<tabela>`` (Parquet, overwrite, sem partição)."""
    df.write.mode("overwrite").parquet(comum.caminho(f"gold/{tabela}"))


# Extrai o elemento ``i`` de um vetor do Spark ML (ex.: ``probability[1]``,
# a probabilidade da classe positiva). O ``probability`` é um
# ``pyspark.ml.linalg.Vector``, que não é indexável por ``F.col(...)[i]``.
def _vetor_elemento(indice: int):
    @F.udf(returnType=DoubleType())
    def _extrair(vetor):
        return float(vetor[indice]) if vetor is not None else None

    return _extrair


# --------------------------------------------------------------------------- #
# §6.1 — Corte anti-vazamento
# --------------------------------------------------------------------------- #

def marcar_corte_add_to_cart(eventos: DataFrame) -> DataFrame:
    """Marca, por evento, se ele é **elegível** para as features (design §6.1).

    Acrescenta duas colunas:

    - ``t_corte`` = menor ``event_ts_ts`` de um ``add_to_cart`` da sessão
      (``null`` se a sessão não tem ``add_to_cart``).
    - ``_elegivel`` (boolean) = ``(t_corte IS NULL OR event_ts_ts < t_corte)`` **e**
      ``event_name`` fora da lista negra ``EVENTOS_CHECKOUT``.

    Dupla barreira: o corte temporal no 1º ``add_to_cart`` e a lista negra de
    eventos de checkout (que vale mesmo quando ``t_corte`` é nulo).
    """
    w = Window.partitionBy("session_id")
    ts_add_to_cart = F.when(
        F.col("event_name") == "add_to_cart", F.col("event_ts_ts")
    )
    com_corte = eventos.withColumn("t_corte", F.min(ts_add_to_cart).over(w))

    elegivel = (
        (F.col("t_corte").isNull() | (F.col("event_ts_ts") < F.col("t_corte")))
        & ~F.col("event_name").isin(EVENTOS_CHECKOUT)
    )
    return com_corte.withColumn("_elegivel", elegivel)


# --------------------------------------------------------------------------- #
# §6.2 — Features por sessão (só pré-corte)
# --------------------------------------------------------------------------- #

def montar_features(eventos: DataFrame) -> DataFrame:
    """Uma linha por ``session_id`` com as features pré-corte (design §6.2).

    Só agrega os eventos elegíveis (``_elegivel``), garantindo que nenhuma feature
    derive de evento ``>= t_corte`` nem de evento de checkout. As contagens de
    eventos de checkout são, por construção, sempre 0 (eles nunca são elegíveis).

    Toda sessão existente na Silver aparece na saída (inclusive as que não têm
    nenhum evento elegível — recebem contagens 0 e flags ``false``), via junção à
    esquerda a partir da lista de sessões. Colunas categóricas ``canal`` e
    ``dispositivo`` vêm de ``atributos_sessao`` (design §1.2).
    """
    marcado = marcar_corte_add_to_cart(eventos)

    def _conta(nome_evento: str):
        return F.sum(
            F.when(
                F.col("_elegivel") & (F.col("event_name") == nome_evento), 1
            ).otherwise(0)
        )

    agregado = (
        marcado.groupBy("session_id")
        .agg(
            F.max(F.when(F.col("_elegivel"), F.col("percent_scrolled"))).alias("scroll_max"),
            F.max(F.when(F.col("_elegivel"), F.col("video_percent"))).alias("video_percent_max"),
            F.sum(F.when(F.col("_elegivel"), 1).otherwise(0)).alias("n_eventos_pre"),
            _conta("view_item_list").alias("n_view_item_list"),
            _conta("select_item").alias("n_select_item"),
            _conta("view_item").alias("n_view_item"),
            _conta("site_search").alias("n_site_search"),
            _conta("file_download").alias("n_file_download"),
            _conta("click").alias("n_click"),
            F.max(
                F.when(F.col("_elegivel") & (F.col("event_name") == "generate_lead"), F.lit(1))
            ).alias("_fez_lead_pre"),
        )
    )

    # Lista canônica de sessões (toda sessão da Silver deve ser escorada, §6.4).
    sessoes = eventos.select("session_id").distinct()
    atributos = atributos_sessao(eventos).select("session_id", "canal", "dispositivo")

    features = (
        sessoes
        .join(agregado, "session_id", "left")
        .join(atributos, "session_id", "left")
        # normaliza nulos: sessão sem eventos elegíveis fica com zeros/false.
        .withColumn("scroll_max", F.coalesce(F.col("scroll_max"), F.lit(0)).cast("int"))
        .withColumn("video_percent_max", F.coalesce(F.col("video_percent_max"), F.lit(0)).cast("int"))
        .withColumn("n_eventos_pre", F.coalesce(F.col("n_eventos_pre"), F.lit(0)).cast("int"))
        .withColumn("n_view_item_list", F.coalesce(F.col("n_view_item_list"), F.lit(0)).cast("int"))
        .withColumn("n_select_item", F.coalesce(F.col("n_select_item"), F.lit(0)).cast("int"))
        .withColumn("n_view_item", F.coalesce(F.col("n_view_item"), F.lit(0)).cast("int"))
        .withColumn("n_site_search", F.coalesce(F.col("n_site_search"), F.lit(0)).cast("int"))
        .withColumn("n_file_download", F.coalesce(F.col("n_file_download"), F.lit(0)).cast("int"))
        .withColumn("n_click", F.coalesce(F.col("n_click"), F.lit(0)).cast("int"))
        .withColumn("fez_lead_pre", F.coalesce(F.col("_fez_lead_pre"), F.lit(0)).cast("int"))
        .withColumn("tem_select_item", (F.col("n_select_item") > 0).cast("int"))
        .withColumn("canal", F.coalesce(F.col("canal"), F.lit("(desconhecido)")))
        .withColumn("dispositivo", F.coalesce(F.col("dispositivo"), F.lit("(desconhecido)")))
        .drop("_fez_lead_pre")
    )
    return features


# --------------------------------------------------------------------------- #
# §6.3 — Rótulo
# --------------------------------------------------------------------------- #

def rotular(eventos: DataFrame) -> DataFrame:
    """Rótulo por sessão (design §6.3; 5.4-6).

    ``label = 1`` se a sessão contém ``purchase``, senão 0. Independe do corte
    anti-vazamento: é o alvo, não feature. Uma linha por ``session_id``.
    """
    return (
        eventos.groupBy("session_id")
        .agg(
            F.max(F.when(F.col("event_name") == "purchase", F.lit(1)).otherwise(0)).alias("label")
        )
        .withColumn("label", F.col("label").cast("int"))
    )


# --------------------------------------------------------------------------- #
# §6.3 — Modelo de propensão (regressão logística) e métricas
# --------------------------------------------------------------------------- #

def _pipeline_features() -> Pipeline:
    """Pipeline de preparação de features: indexação + one-hot das categóricas e
    montagem do vetor ``features`` (design §6.2)."""
    estagios = []
    cols_vetor = list(FEATURES_NUMERICAS)
    for cat in FEATURES_CATEGORICAS:
        idx = StringIndexer(inputCol=cat, outputCol=f"{cat}_idx", handleInvalid="keep")
        ohe = OneHotEncoder(inputCol=f"{cat}_idx", outputCol=f"{cat}_ohe", handleInvalid="keep")
        estagios += [idx, ohe]
        cols_vetor.append(f"{cat}_ohe")
    estagios.append(
        VectorAssembler(inputCols=cols_vetor, outputCol="features", handleInvalid="keep")
    )
    return Pipeline(stages=estagios)


def _pesos_por_classe(treino: DataFrame) -> DataFrame:
    """Adiciona ``peso`` inversamente proporcional à frequência da classe (§6.3).

    Evita o modelo degenerar em "ninguém compra" quando compradores são raros.
    """
    contagens = {l["label"]: l["n"] for l in treino.groupBy("label").agg(F.count(F.lit(1)).alias("n")).collect()}
    total = sum(contagens.values())
    n_classes = max(len(contagens), 1)
    # peso da classe c = total / (n_classes * freq_c)
    pesos = {c: total / (n_classes * n) for c, n in contagens.items()}
    mapa = F.create_map(*[x for c, p in pesos.items() for x in (F.lit(int(c)), F.lit(float(p)))])
    return treino.withColumn("peso", mapa[F.col("label")])


def treinar_e_escorar(features_rotuladas: DataFrame):
    """Treina a regressão logística e escora **todas** as sessões (design §6.3/§6.4).

    Entrada: um DataFrame com as features (§6.2) e a coluna ``label``.

    - split ``randomSplit([0.8, 0.2], seed=42)`` (reprodutível);
    - ``LogisticRegression`` com ``weightCol`` (peso inverso à frequência da classe);
    - escora todas as linhas; ``probabilidade_compra`` = ``probability[1]`` ∈ [0,1];
    - ``conjunto`` ∈ {treino, teste}.

    Devolve ``(propensao, metricas_dict)`` onde ``metricas_dict`` traz
    ``auc, f1, precisao, recall`` (classe positiva, no conjunto de teste) e, para o
    log/RELATORIO, ``n_teste`` e ``positivos_teste`` (NUNCA como linhas da tabela).
    """
    prep = _pipeline_features().fit(features_rotuladas)
    preparado = prep.transform(features_rotuladas).select(
        "session_id", "features", "label"
    )

    treino, teste = preparado.randomSplit([0.8, 0.2], seed=SEED)
    treino = _pesos_por_classe(treino)

    modelo = LogisticRegression(
        featuresCol="features", labelCol="label", weightCol="peso"
    ).fit(treino)

    # Métricas no conjunto de teste, classe positiva (§6.3).
    pred_teste = modelo.transform(teste)
    auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(pred_teste)

    def _multi(metrica: str) -> float:
        return MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName=metrica,
            metricLabel=1.0,
        ).evaluate(pred_teste)

    metricas = {
        "auc": float(auc),
        "f1": float(_multi("fMeasureByLabel")),
        "precisao": float(_multi("precisionByLabel")),
        "recall": float(_multi("recallByLabel")),
        "n_teste": int(teste.count()),
        "positivos_teste": int(teste.where(F.col("label") == 1).count()),
    }

    # Escora TODAS as sessões (§6.4). Marca o conjunto por left-join com o teste.
    ids_teste = teste.select("session_id").distinct().withColumn("_e_teste", F.lit(True))
    escorado = (
        modelo.transform(preparado)
        .join(ids_teste, "session_id", "left")
        .withColumn(
            "conjunto",
            F.when(F.col("_e_teste").isNotNull(), F.lit("teste")).otherwise(F.lit("treino")),
        )
    )

    # probabilidade_compra = probability[1] (elemento da classe positiva), em [0,1].
    propensao = (
        escorado
        .withColumn("probabilidade_compra", _vetor_elemento(1)(F.col("probability")))
        .withColumn("rotulo", F.col("label").cast("int"))
        .select("session_id", "probabilidade_compra", "rotulo", "conjunto")
    )
    return propensao, metricas


def construir_metricas_modelo(spark, metricas: dict) -> DataFrame:
    """Tabela ``metricas_modelo`` em formato longo com EXATAMENTE 4 linhas
    (``auc, f1, precisao, recall``) — apontamento 3 de build-requirements.

    ``n_teste``/``positivos_teste`` NÃO são linhas da tabela (vão para log e
    RELATORIO). A coluna ``features`` repete a lista exata das features em todas as
    linhas (tabela autocontida, design §6.4).
    """
    linhas = [
        ("auc", metricas["auc"], LISTA_FEATURES),
        ("f1", metricas["f1"], LISTA_FEATURES),
        ("precisao", metricas["precisao"], LISTA_FEATURES),
        ("recall", metricas["recall"], LISTA_FEATURES),
    ]
    return spark.createDataFrame(linhas, ["metrica", "valor", "features"])


# --------------------------------------------------------------------------- #
# §6.4 — Segmentação de visitantes (K-Means)
# --------------------------------------------------------------------------- #

def montar_features_visitante(eventos: DataFrame) -> DataFrame:
    """Features por visitante (``user_pseudo_id``) para o K-Means (design §6.4).

    Agrega as sessões de cada visitante: nº de sessões, scroll médio, vídeo médio,
    se comprou alguma vez e nº médio de eventos por sessão. Uma linha por
    ``user_pseudo_id``.
    """
    por_sessao = eventos.groupBy("user_pseudo_id", "session_id").agg(
        F.max("percent_scrolled").alias("_scroll"),
        F.max("video_percent").alias("_video"),
        F.count(F.lit(1)).alias("_n_eventos"),
        F.max(F.when(F.col("event_name") == "purchase", F.lit(1)).otherwise(0)).alias("_comprou"),
    )
    return (
        por_sessao.groupBy("user_pseudo_id")
        .agg(
            F.countDistinct("session_id").alias("n_sessoes"),
            F.coalesce(F.avg("_scroll"), F.lit(0.0)).alias("scroll_medio"),
            F.coalesce(F.avg("_video"), F.lit(0.0)).alias("video_medio"),
            F.max("_comprou").alias("comprou_alguma_vez"),
            F.coalesce(F.avg("_n_eventos"), F.lit(0.0)).alias("n_eventos_medio"),
        )
        .withColumn("n_sessoes", F.col("n_sessoes").cast("double"))
        .withColumn("comprou_alguma_vez", F.col("comprou_alguma_vez").cast("double"))
    )


def segmentar_visitantes(features_visitante: DataFrame):
    """Clusteriza os visitantes por K-Means, escolhendo K por silhueta (§6.4).

    Padroniza as features com ``StandardScaler`` e varre ``K ∈ {2..5}`` com
    ``ClusteringEvaluator`` (silhueta), ficando com o melhor K. Devolve
    ``(segmentos, perfil, k_escolhido, silhueta)``:

    - ``segmentos`` = (``user_pseudo_id``, ``segmento`` int);
    - ``perfil`` = (``segmento``, ``n_visitantes``, médias de cada feature).
    """
    montador = VectorAssembler(
        inputCols=FEATURES_VISITANTE, outputCol="_vetor", handleInvalid="keep"
    )
    scaler = StandardScaler(
        inputCol="_vetor", outputCol="features", withMean=True, withStd=True
    )
    prep = Pipeline(stages=[montador, scaler]).fit(features_visitante)
    base = prep.transform(features_visitante).cache()

    n_visitantes = base.count()
    avaliador = ClusteringEvaluator(
        featuresCol="features", predictionCol="segmento", metricName="silhouette"
    )

    melhor = None  # (silhueta, k, modelo)
    # K não pode exceder o nº de visitantes distintos.
    k_max = min(K_MAX, max(n_visitantes, 1))
    for k in range(K_MIN, k_max + 1):
        if k > n_visitantes:
            break
        modelo_k = KMeans(
            featuresCol="features", predictionCol="segmento", k=k, seed=SEED
        ).fit(base)
        pred_k = modelo_k.transform(base)
        # a silhueta exige ao menos 2 clusters efetivamente preenchidos.
        if pred_k.select("segmento").distinct().count() < 2:
            continue
        sil = avaliador.evaluate(pred_k)
        if melhor is None or sil > melhor[0]:
            melhor = (sil, k, modelo_k)

    if melhor is None:
        # fallback: poucos visitantes para avaliar silhueta — tudo no segmento 0.
        segmentos = base.withColumn("segmento", F.lit(0)).select(
            "user_pseudo_id", "segmento"
        )
        k_escolhido, silhueta = 1, float("nan")
    else:
        silhueta, k_escolhido, modelo = melhor
        segmentos = modelo.transform(base).select("user_pseudo_id", "segmento")

    segmentos = segmentos.withColumn("segmento", F.col("segmento").cast("int"))

    # Perfil médio de cada segmento (design §6.4; tabela #13 em par).
    medias = [F.round(F.avg(c), 4).alias(c) for c in FEATURES_VISITANTE]
    perfil = (
        features_visitante.join(segmentos, "user_pseudo_id", "inner")
        .groupBy("segmento")
        .agg(F.count(F.lit(1)).cast("long").alias("n_visitantes"), *medias)
        .orderBy("segmento")
    )

    base.unpersist()
    return segmentos, perfil, k_escolhido, silhueta


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def main() -> int:
    spark = criar_sessao("gold_ml")
    try:
        exigir_camada(spark, glob_eventos("silver"), "python pipeline/silver.py")  # R2
        eventos = ler_eventos(spark, "silver")

        # Propensão (regressão logística, sem vazamento).
        features = montar_features(eventos)
        rotulos = rotular(eventos)
        features_rotuladas = features.join(rotulos, "session_id", "left").withColumn(
            "label", F.coalesce(F.col("label"), F.lit(0)).cast("int")
        )
        propensao, metricas = treinar_e_escorar(features_rotuladas)
        metricas_modelo = construir_metricas_modelo(spark, metricas)

        # Segmentação de visitantes (K-Means).
        features_visitante = montar_features_visitante(eventos)
        segmentos, perfil, k_escolhido, silhueta = segmentar_visitantes(features_visitante)

        _gravar(propensao, "propensao_sessao")
        _gravar(metricas_modelo, "metricas_modelo")
        _gravar(segmentos, "segmentos_visitantes")
        _gravar(perfil, "segmentos_perfil")

        titulo("Gold ML")
        print(f"propensao_sessao:     {propensao.count()} sessões escoradas")
        print(f"metricas_modelo:      {metricas_modelo.count()} linhas (auc, f1, precisao, recall)")
        print(
            "  auc={auc:.4f}  f1={f1:.4f}  precisao={precisao:.4f}  recall={recall:.4f}".format(**metricas)
        )
        print(
            f"  (contexto, fora da tabela) n_teste={metricas['n_teste']}"
            f"  positivos_teste={metricas['positivos_teste']}"
        )
        print(f"segmentos_visitantes: {segmentos.count()} visitantes  (K={k_escolhido}, silhueta={silhueta:.4f})")
        print(f"segmentos_perfil:     {perfil.count()} segmentos")
        print(f"\nfeatures: {LISTA_FEATURES}")
        print(
            "Limitações (RELATORIO): base sintética (gerador com seed); compradores raros "
            "(classe desbalanceada); features só do pré-carrinho; métricas refletem o gerador, "
            "não tráfego real."
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
