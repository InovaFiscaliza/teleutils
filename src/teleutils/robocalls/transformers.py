"""Transformação e padronização de registros CDR extraídos para análise de chamadas abusivas.

Este módulo realiza a transformação de registros CDR brutos (da camada de extração)
em um formato padronizado e enriquecido, adequado para análise de padrões de
chamadas abusivas e robocalls. As transformações incluem: normalização de números
telefônicos, conversão de timestamps, cálculo de indicadores binários (chamada curta,
caixa postal, autenticação) e seleção de colunas finais.

Cada formato de CDR (Ericsson, TIM VoLTE, Vivo VoLTE) possui um método
de transformação específico que aplica um pipeline padrão comum a todos e, depois,
adições específicas de formato (ex: extração de autenticação STIR, detecção de
correio de voz).

O resultado da transformação é um DataFrame com as seguintes colunas:
- referencia: Identificador único da chamada
- tipo_de_chamada: Tipo da chamada (TER, FORv, etc.)
- data_hora: Timestamp da chamada
- numero_de_a_formatado: Número originador padronizado (CN+PREFIXO+MCDU)
- numero_de_b_formatado: Número destinatário padronizado (CN+PREFIXO+MCDU)
- hora_da_chamada: Hora cheia em formato YYYYMMDDHH
- duracao_da_chamada: Duração em segundos (inteiro)
- chamada_curta: Flag (0 ou 1) indicando duração <= limiar_chamada_ofensora
- chamada_autenticada: Flag (-1=falhou, 0=não verificada, 1=válida)
- chamada_caixa_postal: Flag (0 ou 1) indicando encaminhamento ao correio de voz

Nota:
    Este módulo depende de `teleutils.preprocessing.normalize_number` para padronizar
    números telefônicos. Os transformadores aplicam operações Apache Spark e usam
    pandas_udf para vetorização eficiente de funções Python.
"""

from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf

from teleutils._logging import log_operation
from teleutils.preprocessing import normalize_number

logger = logging.getLogger(__name__)

_return_schema = T.StructType(
    [
        T.StructField("numero_formatado", T.StringType(), True),
        T.StructField("numero_valido", T.BooleanType(), True),
    ]
)


@pandas_udf(_return_schema)
def _spark_normalize_number(number_series: pd.Series) -> pd.DataFrame:
    """Normaliza números telefônicos em formato vetorizado usando pandas_udf.

    UDF (User Defined Function) vetorizada que processa um lote (batch) de números
    telefônicos, aplicando a função `normalize_number` a cada um deles. Retorna um
    DataFrame com duas colunas: o número formatado e um flag booleano indicando
    se o número é válido.

    Esta função é registrada como pandas_udf no Spark para melhor performance,
    pois permite processamento vetorizado em Python sem overhead de serialização
    por linha.

    Parâmetros:
        number_series (pd.Series): Série pandas contendo números telefônicos como strings.

    Retorna:
        pd.DataFrame: DataFrame com colunas 'numero_formatado' (str) e 'numero_valido' (bool).
            - numero_formatado: Número padronizado conforme plano de numeração ou como
              informado pela prestadora
            - numero_valido: True se o número está aderente ao plano de numeração

    Exemplo:
        >>> # Uso dentro de transformação Spark:
        >>> df = spark.createDataFrame(
        ...     [("11987654321",), ("1234",)], ["numero_de_a"]
        ... )
        >>> result = df.withColumn(
        ...     "numero_formatado",
        ...     _spark_normalize_number("numero_de_a").getField("numero_formatado")
        ... )
    """
    # Processar em batch (vetorizado)
    results = [normalize_number(n) for n in number_series]

    return pd.DataFrame(
        results,
        columns=[
            "numero_formatado",
            "numero_valido",
        ],
    )


class RoboCallsTransformer:
    """Transforma registros CDR extraídos para análise de chamadas abusivas.

    Centraliza a lógica de transformação de diferentes formatos CDR, normalizando
    números, parseando timestamps, e enriquecendo registros com indicadores binários
    de padrão de abuso (chamada curta, caixa postal, autenticação).

    A classe declara constantes privadas para comportamento esperado de campos
    específicos (ex: valores de autenticação) e um limiar configurável para detectar
    "chamadas curtas" (indicador de possível chamada abusiva automática).

    Atributos:
        spark (SparkSession): Sessão Spark para operações de I/O.
        limiar_chamada_ofensora (int): Duração máxima em segundos para classificar
            uma chamada como "curta". Padrão: 6 segundos.
            Uma chamada com duracao_da_chamada <= limiar_chamada_ofensora terá
            chamada_curta = 1.
    """

    # Valores possíveis de chamada_autenticada:
    #  0 = não autenticada (é nulo)
    # +1 = autenticação válida (contém TN-Validation-Pa)
    # -1 = autenticação falhou (em qualquer outro caso)
    _AUTH_NONE = 0
    _AUTH_PASS = 1
    _AUTH_FAIL = -1

    _LIMIAR_CHAMADA_OFENSORA = 6

    _TRANSFORM_MAP: dict[str, str] = {
        "ericsson": "transform_cdr_ericsson",
        "tim_volte": "transform_cdr_tim_volte",
        "vivo_volte": "transform_cdr_vivo_volte",
    }

    _TRANSFORMED_COLUMNS = [
        "referencia",
        "tipo_de_chamada",
        "data_hora",
        "numero_de_a_formatado",
        "numero_de_b_formatado",
        "hora_da_chamada",
        "duracao_da_chamada",
        "chamada_curta",
        "chamada_autenticada",
        "chamada_caixa_postal",
    ]

    def __init__(
        self,
        spark: SparkSession,
        limiar_chamada_ofensora: int = _LIMIAR_CHAMADA_OFENSORA,
    ):
        """Inicializa o transformador com sessão Spark e limiar de chamada curta.

        Parâmetros:
            spark (SparkSession): Sessão Spark ativa para operações de I/O.
            limiar_chamada_ofensora (int, opcional): Duração máxima em segundos para
                considerar uma chamada como "curta". Padrão: 6 segundos.

        Exemplo:
            >>> transformer = RoboCallsTransformer(spark, limiar_chamada_ofensora=5)
        """
        self.spark = spark
        self.limiar_chamada_ofensora = limiar_chamada_ofensora

    def _format_columns(self, df, date_time_fmt: str = "yyyy-MM-dd HH-mm-ss"):
        """Padroniza tipos e formatos de colunas de data/hora e duração.

        Aplica transformações: conversão de duração para inteiro (null -> 0),
        concateção de data e hora, parsing para timestamp, e extração da hora cheia
        em formato YYYYMMDDHH.

        Parâmetros:
            df (DataFrame): DataFrame com colunas _data, _hora e duracao_da_chamada.
            date_time_fmt (str, opcional): Formato de data/hora do arquivo. Padrão:
                "yyyy-MM-dd HH-mm-ss". Exemplos: "yyyy-MM-dd HH:mm:ss", "yyyyMMdd HHmmss".

        Retorna:
            DataFrame: DataFrame com colunas adicionadas/modificadas:
                - duracao_da_chamada (int): Duração em segundos; 0 se null
                - data_hora (timestamp): Timestamp da chamada
                - hora_da_chamada (str): Hora cheia em formato YYYYMMDDHH

        Exemplo:
            >>> df_raw = spark.createDataFrame(
            ...     [("2026-01-21", "14:00:00", "120")],
            ...     ["_data", "_hora", "duracao_da_chamada"]
            ... )
            >>> df_formatted = transformer._format_columns(df_raw, "yyyy-MM-dd HH:mm:ss")
            >>> df_formatted.select("hora_da_chamada").collect()
            [Row(hora_da_chamada='2026012114')]
        """

        if "data_hora" not in df.columns:
            df = df.withColumn(
                "data_hora", F.concat_ws(" ", F.col("_data"), F.col("_hora"))
            )

        return (
            df.withColumn(
                "duracao_da_chamada",
                F.coalesce(F.col("duracao_da_chamada").cast(T.IntegerType()), F.lit(0)),
            )
            .withColumn("data_hora", F.to_timestamp(F.col("data_hora"), date_time_fmt))
            .withColumn(
                "hora_da_chamada", F.date_format(F.col("data_hora"), "yyyyMMddHH")
            )
        )

    def _format_numbers(self, df):
        """Normaliza números A e B usando pandas_udf de normalização.

        Aplica a função vetorizada `_spark_normalize_number` aos campos numero_de_a
        e numero_de_b, extraindo o campo "numero_formatado" de cada resultado.

        Parâmetros:
            df (DataFrame): DataFrame contendo colunas numero_de_a e numero_de_b.

        Retorna:
            DataFrame: DataFrame com colunas adicionadas:
                - numero_de_a_formatado (str): Número originador padronizado
                  (formato CN+PREFIXO+MCDU ou como informado pela prestadora)
                - numero_de_b_formatado (str): Número destinatário padronizado
                  (formato CN+PREFIXO+MCDU ou como informado pela prestadora)

        Exemplo:
            >>> df_raw = spark.createDataFrame(
            ...     [("11987654321", "1140001234")],
            ...     ["numero_de_a", "numero_de_b"]
            ... )
            >>> df_formatted = transformer._format_numbers(df_raw)
        """
        return df.withColumn(
            "numero_de_a_formatado",
            _spark_normalize_number("numero_de_a").getField("numero_formatado"),
        ).withColumn(
            "numero_de_b_formatado",
            _spark_normalize_number("numero_de_b").getField("numero_formatado"),
        )

    def _add_chamada_curta(self, df):
        """Marca chamadas com duração inferior ou igual ao limiar como "curtas".

        Adiciona coluna chamada_curta com valor 1 se duracao_da_chamada <=
        limiar_chamada_ofensora, caso contrário 0.

        Uma "chamada curta" é indicadora de possível padrão abusivo (robocall,
        chamada automática, etc.), pois a maioria das chamadas legítimas tem duração
        maior que o limiar.

        Parâmetros:
            df (DataFrame): DataFrame contendo coluna duracao_da_chamada (int).

        Retorna:
            DataFrame: DataFrame com coluna adicionada:
                - chamada_curta (int): 1 se duração <= limiar; 0 caso contrário

        Exemplo:
            >>> df = spark.createDataFrame(
            ...     [(3,), (10,), (6,)],
            ...     ["duracao_da_chamada"]
            ... )
            >>> df_marked = transformer._add_chamada_curta(df)
            >>> # Com limiar_chamada_ofensora = 6:
            >>> # Retorna: [(1,), (0,), (1,)]
        """
        return df.withColumn(
            "chamada_curta",
            F.when(
                F.col("duracao_da_chamada") <= self.limiar_chamada_ofensora, 1
            ).otherwise(F.lit(0)),
        )

    def _add_chamada_autenticada(self, df):
        """Classifica autenticação STIR/SHAKEN do registro CDR.

        Interpreta a coluna autenticacao (origem: campo SIP ou HTTP header) e
        classifica em três categorias:
        - 0: Autenticação não verificada (campo nulo)
        - 1: Autenticação bem-sucedida (contém string "TN-Validation-Pa")
        - -1: Autenticação falhou (qualquer outro valor não nulo)

        Parâmetros:
            df (DataFrame): DataFrame contendo coluna autenticacao (str ou null).

        Retorna:
            DataFrame: DataFrame com coluna adicionada:
                - chamada_autenticada (int): -1 (falhou), 0 (não verificada) ou
                  1 (bem-sucedida)

        Nota:
            Este método é relevante principalmente para CDRs do tipo STIR/SHAKEN
            (ex: TIM VoLTE, Vivo VoLTE). Outros formatos podem ter essa coluna como
            null em todos os registros.

        Exemplo:
            >>> df = spark.createDataFrame(
            ...     [(None,), ("TN-Validation-Pass",), ("invalid",)],
            ...     ["autenticacao"]
            ... )
            >>> df_auth = transformer._add_chamada_autenticada(df)
            >>> # Retorna: [(0,), (1,), (-1,)]
        """
        return df.withColumn(
            "chamada_autenticada",
            F.when(F.col("autenticacao").isNull(), self._AUTH_NONE)
            .when(F.col("autenticacao").contains("TN-Validation-Pa"), self._AUTH_PASS)
            .otherwise(F.lit(self._AUTH_FAIL)),
        )

    def _apply_standard_pipeline(
        self, df: DataFrame, date_time_fmt: str = "yyyy-MM-dd HH-mm-ss"
    ) -> DataFrame:
        """Aplica sequência padrão de transformações a um CDR.

        Pipeline genérico reutilizado por todos os métodos de transformação específicos
        de formato. Executa em ordem:
        1. Padronização de tipos (data/hora e duração)
        2. Normalização de números telefônicos
        3. Detecção de chamadas curtas

        Parâmetros:
            df (DataFrame): DataFrame extraído contendo colunas intermediárias (_data,
                _hora, numero_de_a, numero_de_b, duracao_da_chamada).
            date_time_fmt (str, opcional): Formato de data/hora. Padrão:
                "yyyy-MM-dd HH-mm-ss".

        Retorna:
            DataFrame: DataFrame com transformações aplicadas (mas sem seleção final
                de colunas).

        Exemplo:
            >>> df_extracted = spark.read.parquet("cdr_ericsson_extracted/")
            >>> df_transformed = transformer._apply_standard_pipeline(
            ...     df_extracted, "yyyy-MM-dd HH:mm:ss"
            ... )
        """
        df = self._format_columns(df, date_time_fmt)
        df = self._format_numbers(df)
        df = self._add_chamada_curta(df)
        return df

    def _write_parquet(self, df: DataFrame, target_file: str) -> None:
        """Grava DataFrame transformado em formato parquet sem particionamento.

        Diferente da camada de extração (particionada por tipo_de_chamada), a camada
        transformada é gravada flat porque as operações subsequentes no analyzer
        agrupam por numero_de_a_formatado + hora_da_chamada, não por tipo_de_chamada.
        O particionamento por tipo_de_chamada não traria pruning real nessa etapa.

        Parâmetros:
            df (DataFrame): DataFrame com colunas transformadas.
            target_file (str): Caminho para o diretório parquet de saída.

        Retorna:
            None

        Nota:
            A coluna tipo_de_chamada é convertida explicitamente para StringType
            antes da escrita, garantindo consistência de tipo quando o DataFrame foi
            criado com tipos inferenciais.
        """
        df.withColumn(
            "tipo_de_chamada", F.col("tipo_de_chamada").cast(T.StringType())
        ).write.mode("overwrite").parquet(target_file)

    @log_operation
    def transform_cdr_ericsson(self, source_file: str, target_file: str):
        """Transforma CDR no formato Ericsson.

        Aplica pipeline padrão de transformação e, depois, inicializa campos
        específicos do formato Ericsson (autenticação e caixa postal como 0,
        pois Ericsson não fornece esses dados). Filtra apenas chamadas tipo "TER"
        (terminadas).

        Parâmetros:
            source_file (str): Caminho para o diretório parquet Ericsson extraído.
            target_file (str): Caminho para o diretório parquet transformado de saída.

        Retorna:
            DataFrame: DataFrame com colunas definidas em _TRANSFORMED_COLUMNS.

        Exemplo:
            >>> transformer = RoboCallsTransformer(spark)
            >>> df = transformer.transform_cdr_ericsson(
            ...     source_file="parquet/ericsson_extracted",
            ...     target_file="parquet/ericsson_transformed"
            ... )
        """
        date_time_fmt = "yyyy-MM-dd HH:mm:ss"
        df = self.spark.read.parquet(source_file).filter(
            F.col("tipo_de_chamada") == "TER"
        )
        df = self._apply_standard_pipeline(df, date_time_fmt)
        df = (
            df.withColumn("chamada_autenticada", F.lit(0))
            .withColumn("chamada_caixa_postal", F.lit(0))
            .select(self._TRANSFORMED_COLUMNS)
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_tim_volte(self, source_file: str, target_file: str):
        """Transforma CDR no formato TIM VoLTE.

        Realiza duas leituras de source_file: uma para identificar chamadas
        encaminhadas ao correio de voz (tipo 'FORv' com numero_de_b iniciando em
        "5505" e tamanho 6 dígitos) e outra para as chamadas principais (tipo 'TERv').
        Depois realiza join left para marcar chamadas de caixa postal.

        Aplica pipeline padrão de transformação e inicializa autenticacao como 0.

        Parâmetros:
            source_file (str): Caminho para o diretório parquet TIM VoLTE extraído,
                particionado por tipo_de_chamada (ex: tipo_de_chamada=FORv/).
            target_file (str): Caminho para o diretório parquet transformado de saída.

        Retorna:
            DataFrame: DataFrame com colunas definidas em _TRANSFORMED_COLUMNS.

        Nota:
            - A detecção de caixa postal usa heurística: tipo 'FORv' + numero_de_b
              com padrão "5505" e 6 dígitos totais.
            - Source_file deve estar particionado por tipo_de_chamada para que
              os filtros usem partition pruning; sem isso, será lido integralmente
              duas vezes.
            - Uma mesma referencia pode ter múltiplas registros (tipo FORv e TERv);
              o join left garante que apenas registros TERv apareçam no resultado
              final, com flag chamada_caixa_postal preenchido baseado na existência
              de referencia análoga no tipo FORv.

        Exemplo:
            >>> transformer = RoboCallsTransformer(spark)
            >>> df = transformer.transform_cdr_tim_volte(
            ...     source_file="parquet/tim_volte_extracted",
            ...     target_file="parquet/tim_volte_transformed"
            ... )
        """
        df_voice_mail = (
            self.spark.read.parquet(source_file)
            .filter(F.col("tipo_de_chamada") == "FORv")
            .filter(
                (F.col("numero_de_b").startswith("5505"))
                & (F.length("numero_de_b") == 6)
            )
            .select("referencia")
            .distinct()
            .withColumn("chamada_caixa_postal", F.lit(1))
        )

        df = self.spark.read.parquet(source_file).filter(
            F.col("tipo_de_chamada") == "TERv"
        )

        df = self._apply_standard_pipeline(df)
        df = self._add_chamada_autenticada(df)
        df = (
            df.join(df_voice_mail, on="referencia", how="left")
            .withColumn(
                "chamada_caixa_postal",
                F.coalesce(F.col("chamada_caixa_postal"), F.lit(0)),
            )
            .select(self._TRANSFORMED_COLUMNS)
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_vivo_volte(self, source_file: str, target_file: str):
        """Transforma CDR no formato Vivo VoLTE.

        Realiza duas leituras de source_file: uma para identificar chamadas
        encaminhadas ao correio de voz (tipo '3', com numero_de_a_formatado ==
        numero_de_b_formatado usando substring dos últimos 11 dígitos) e outra
        para as chamadas principais (tipo '4'). Depois realiza join left usando
        chaves (referencia, data_hora, numero_de_b_formatado).

        Aplica pipeline padrão e processa autenticação.

        Parâmetros:
            source_file (str): Caminho para o diretório parquet Vivo VoLTE extraído,
                particionado por tipo_de_chamada (ex: tipo_de_chamada=3/).
            target_file (str): Caminho para o diretório parquet transformado de saída.

        Retorna:
            DataFrame: DataFrame com colunas definidas em _TRANSFORMED_COLUMNS.

        Nota:
            - Detecção de caixa postal usa heurística: tipo '3' + comparação de
              últimos 11 dígitos de numero_de_a_formatado com numero_de_b_formatado
              (chamadas para si mesmo são consideradas correio de voz).
            - Campo numero_de_a no tipo '4' contém formato concatenado:
              "numero_de_a;campo_autenticacao"; é feito split por ";" para separar.
            - Source_file deve estar particionado por tipo_de_chamada para partition
              pruning; sem isso, será lido integralmente duas vezes.
            - Join usa três colunas (referencia, data_hora, numero_de_b_formatado)
              para relacionar registro tipo '3' ao tipo '4', garantindo precisão.

        Exemplo:
            >>> transformer = RoboCallsTransformer(spark)
            >>> df = transformer.transform_cdr_vivo_volte(
            ...     source_file="parquet/vivo_volte_extracted",
            ...     target_file="parquet/vivo_volte_transformed"
            ... )
        """
        date_time_fmt = "yyyyMMdd HHmmss"
        voice_mail_columns_to_keep = [
            "referencia",
            "data_hora",
            "numero_de_b_formatado",
            "chamada_caixa_postal",
        ]
        df_voice_mail = (
            self.spark.read.parquet(source_file)
            .filter(F.col("tipo_de_chamada") == "3")
            .withColumn("numero_de_a_formatado", F.col("numero_de_a").substr(-11, 11))
            .withColumn("numero_de_b_formatado", F.col("numero_de_b").substr(-11, 11))
            .filter(F.col("numero_de_a_formatado") == F.col("numero_de_b_formatado"))
            .withColumn("chamada_caixa_postal", F.lit(1))
        )
        df_voice_mail = self._format_columns(df_voice_mail, date_time_fmt).select(
            voice_mail_columns_to_keep
        )

        df = (
            self.spark.read.parquet(source_file)
            .filter(F.col("tipo_de_chamada") == "4")
            .withColumn("autenticacao", F.split(F.col("numero_de_a"), ";").getItem(1))
            .withColumn("numero_de_a", F.split(F.col("numero_de_a"), ";").getItem(0))
        )
        df = self._apply_standard_pipeline(df, date_time_fmt)
        df = self._add_chamada_autenticada(df)
        df = (
            df.join(
                df_voice_mail,
                on=["referencia", "data_hora", "numero_de_b_formatado"],
                how="left",
            )
            .withColumn(
                "chamada_caixa_postal",
                F.coalesce(F.col("chamada_caixa_postal"), F.lit(0)),
            )
            .select(self._TRANSFORMED_COLUMNS)
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_claro_nokia(self, source_file: str, target_file: str):
        """Transforma CDR no formato Claro Nokia.

        Aplica pipeline padrão de transformação e, depois, inicializa campos
        específicos do formato Claro Nokia (autenticação como 0,
        pois Claro Nokia não fornece esses dados).

        Parâmetros:
            source_file (str): Caminho para o diretório parquet Ericsson extraído.
            target_file (str): Caminho para o diretório parquet transformado de saída.

        Retorna:
            DataFrame: DataFrame com colunas definidas em _TRANSFORMED_COLUMNS.

        Exemplo:
            >>> transformer = RoboCallsTransformer(spark)
            >>> df = transformer.transform_cdr_ericsson(
            ...     source_file="parquet/ericsson_extracted",
            ...     target_file="parquet/ericsson_transformed"
            ... )
        """
        date_time_fmt = "yyyy-MM-dd HH:mm:ss"

        df_ptc = (
            self.spark.read.parquet(source_file)
            .filter(F.col("tipo_de_chamada") == "PTC")
            .select("referencia", "numero_de_a")
            .withColumn("chamada_ptc", F.lit(1))
        )

        df_poc_without_ptc = (
            self.spark.read.parquet(source_file)
            .filter(F.col("tipo_de_chamada") == "POC")
            .join(df_ptc, on=["referencia", "numero_de_a"], how="left")
            .filter(F.col("chamada_ptc").isNull())
        )

        df_voice_mail = (
            self.spark.read.parquet(source_file)
            .filter(F.col("tipo_de_chamada")=="FOR")
            .withColumn("chamada_caixa_postal", F.lit(1))
            .select("referencia", "numero_de_a", "chamada_caixa_postal")
        )

        tipo_de_chamada_to_keep = ["MTC", "UCA", "FOR", "MOC"]
        df = (
            self.spark.read.parquet(source_file)
            .filter(F.col("tipo_de_chamada").isin(tipo_de_chamada_to_keep))
            .unionByName(df_poc_without_ptc, allowMissingColumns=True)
            .dropDuplicates(["referencia", "numero_de_a"])
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        
        df = (
            df.join(df_voice_mail, on=["referencia", "numero_de_a"], how="left")
            .withColumn(
                "chamada_caixa_postal",
                F.coalesce(F.col("chamada_caixa_postal"), F.lit(0)),
            )
            .withColumn("chamada_autenticada", F.lit(0))
            .select(self._TRANSFORMED_COLUMNS)
        )

        df = (
            df.join(df_voice_mail, on="referencia", how="left")
            .withColumn(
                "chamada_caixa_postal",
                F.coalesce(F.col("chamada_caixa_postal"), F.lit(0)),
            )
            .select(self._TRANSFORMED_COLUMNS)
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
