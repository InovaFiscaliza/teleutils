"""Módulo teleutils.core.transformers.

Objetivo principal:
    Transformar dados de CDR previamente extraídos para um formato analítico
    padronizado, aplicando normalização de números, padronização temporal e
    enriquecimento com informações de autenticação.

Responsabilidades:
    - Executar pipeline comum de transformação para diferentes layouts de CDR.
    - Normalizar números telefônicos por meio de UDF vetorizada em pandas.
    - Consolidar regras de autenticação (TN-Validation) em campo categórico.
    - Selecionar e renomear colunas para o contrato de saída do domínio.

Principais funcionalidades:
    - Conversão de data/hora para timestamp Spark.
    - Padronização e validação de número de origem e destino.
    - Unificação de schema de saída para múltiplas prestadoras.
    - Escrita parquet em modo overwrite para reprocessamento determinístico.

Dependências relevantes:
    - pyspark.sql.SparkSession
    - pyspark.sql.DataFrame
    - pyspark.sql.functions
    - pyspark.sql.types
    - pandas
    - teleutils.preprocessing.normalize_number

Example:
    >>> transformer = CDRTransformer(spark)
    >>> df = transformer.transform_cdr_ericsson(
    ...     source_file="/dados/staging/cdr_ericsson",
    ...     target_file="/dados/gold/cdr_ericsson"
    ... )
"""

from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf  # type: ignore

from teleutils._config import MAX_RECORDS_PER_FILE
from teleutils._logging import log_operation
from teleutils.preprocessing import normalize_number

logger = logging.getLogger(__name__)

_return_schema = T.StructType(
    [
        T.StructField("numero_formatado", T.StringType(), True),
        T.StructField("numero_valido", T.BooleanType(), True),
    ]
)


@pandas_udf(_return_schema)  # type: ignore
def _spark_normalize_number(number_series: pd.Series) -> pd.DataFrame:
    """Normaliza números telefônicos em lote para uso em pipeline Spark.

    Esta UDF pandas recebe uma série de números e aplica a função de domínio
    ``normalize_number`` de forma vetorizada (batch), retornando estrutura
    tabular com número formatado e indicador de validade.

    Args:
        number_series: Série pandas com números telefônicos em formato bruto.

    Returns:
        pd.DataFrame: DataFrame com duas colunas:
            - numero_formatado (str | None)
            - numero_valido (bool | None)

    Notes:
        - A escolha por pandas UDF reduz overhead em comparação com UDF linha a
          linha, melhorando desempenho em grandes volumes.
        - Anotação de manutenção: alterações no contrato de retorno exigem
          atualização coordenada de ``_return_schema`` e dos acessos às colunas
          estruturadas em ``_format_numbers``.
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


class CDRTransformer:
    """Aplica regras de transformação para CDRs extraídos em parquet.

    Classe responsável por converter datasets intermediários de extração para um
    schema de consumo padronizado no domínio. Os métodos públicos representam
    entradas por tipo de layout/origem, enquanto métodos privados concentram
    etapas reutilizáveis de transformação.

    Attributes:
        spark: Sessão Spark utilizada para leitura, transformação e escrita.

    Notes:
        - O pipeline padrão é centralizado em ``_apply_standard_pipeline`` para
          evitar divergência entre formatos.
        - Ponto de extensão: novos formatos devem reutilizar o pipeline comum e
          ajustar somente pré-processamentos específicos quando necessários.
    """

    def __init__(
        self,
        spark: SparkSession,
    ):
        """Inicializa o transformador com sessão Spark ativa.

        Args:
            spark: Sessão Spark compartilhada pelo pipeline de transformação.
        """

        self.spark = spark

    def _format_date_time(self, df, date_time_fmt: str = "yyyy-MM-dd HH-mm-ss"):
        """Padroniza campos temporais e normaliza duração.

        Objetivo da operação:
            Garantir que o dataset possua coluna ``data_hora`` em formato
            timestamp e que ``duracao`` esteja tipada como inteiro, com fallback
            para zero quando ausente ou inválida.

        Args:
            df: DataFrame Spark de entrada.
            date_time_fmt: Máscara de parsing para conversão de ``data_hora``.

        Returns:
            DataFrame: DataFrame com ``duracao`` normalizada e ``data_hora``
            convertida para timestamp.

        Notes:
            - Regra de negócio: duração inválida é tratada como 0 para manter
              consistência em métricas downstream.
            - Quando ``data_hora`` não existe, ela é construída por concatenação
              de ``_data`` e ``_hora``.
        """

        if "data_hora" not in df.columns:
            df = df.withColumn(
                "data_hora", F.concat_ws(" ", F.col("_data"), F.col("_hora"))
            )

        return df.withColumn(
            "duracao",
            F.coalesce(F.col("duracao").cast(T.IntegerType()), F.lit(0)),
        ).withColumn("data_hora", F.to_timestamp(F.col("data_hora"), date_time_fmt))

    def _format_numbers(self, df):
        """Normaliza números de origem/destino e adiciona indicadores de validade.

        Args:
            df: DataFrame Spark contendo ao menos ``numero_origem`` e
                ``numero_destino``.

        Returns:
            DataFrame: DataFrame com colunas formatadas e flags booleanas de
            validade para origem e destino.

        Notes:
            - A UDF retorna struct; por isso são criadas colunas temporárias
              intermediárias e depois expandidas.
            - Efeito colateral lógico: colunas temporárias são removidas ao final
              para manter o schema limpo.
        """
        return (
            df.withColumn(
                "_numero_origem_formatado",
                _spark_normalize_number("numero_origem"),  # type: ignore
            )
            .withColumn(
                "_numero_destino_formatado",
                _spark_normalize_number("numero_destino"),  # type: ignore
            )
            .withColumn(
                "numero_origem_formatado",
                F.col("_numero_origem_formatado.numero_formatado"),
            )
            .withColumn(
                "numero_origem_valido", F.col("_numero_origem_formatado.numero_valido")
            )
            .withColumn(
                "numero_destino_formatado",
                F.col("_numero_destino_formatado.numero_formatado"),
            )
            .withColumn(
                "numero_destino_valido",
                F.col("_numero_destino_formatado.numero_valido"),
            )
            .drop("_numero_origem_formatado")
            .drop("_numero_destino_formatado")
        )

    def _add_tn_validation_status(self, df):
        """Deriva status textual de autenticação a partir de ``_autenticacao``.

        Args:
            df: DataFrame Spark com ou sem coluna ``_autenticacao``.

        Returns:
            DataFrame: DataFrame com coluna ``autenticacao`` categorizada.

        Notes:
            - Regra de negócio: quando ``_autenticacao`` não existe, o status é
              definido como nulo.
            - A classificação usa prefixos ``verstat=...`` para manter aderência
              ao padrão atualmente recebido dos fornecedores.
            - Anotação de manutenção: novos códigos de autenticação devem ser
              adicionados nesta cadeia de ``when``.
        """
        if "_autenticacao" in df.columns:
            df = df.withColumn(
                "autenticacao",
                F.when(
                    F.col("_autenticacao").startswith("verstat=TN-Validation-P"),
                    "TN-Validation-Passed",
                )
                .when(
                    F.col("_autenticacao").startswith("verstat=TN-Validation-F"),
                    "TN-Validation-Failed",
                )
                .when(
                    F.col("_autenticacao").startswith("verstat=No-TN-Validation"),
                    "No-TN-Validation",
                )
                .otherwise(None),
            )
        else:
            df = df.withColumn("autenticacao", F.lit(None).cast(T.StringType()))

        return df

    def _apply_standard_pipeline(
        self, df: DataFrame, date_time_fmt: str = "yyyy-MM-dd HH-mm-ss"
    ) -> DataFrame:
        """Executa pipeline comum de transformação para todos os layouts.

        Fluxo de processamento:
            1. Padronização temporal e duração.
            2. Normalização de números telefônicos.
            3. Enriquecimento de status de autenticação.

        Args:
            df: DataFrame de entrada.
            date_time_fmt: Formato de data/hora esperado para parsing.

        Returns:
            DataFrame: DataFrame transformado conforme regras padrão.

        Notes:
            Decisão arquitetural: centralizar o pipeline reduz risco de regras
            divergentes entre prestadoras e facilita manutenção evolutiva.
        """

        df = self._format_date_time(df, date_time_fmt)
        df = self._format_numbers(df)
        df = self._add_tn_validation_status(df)

        return df

    def _select_transformed_columns(self, df: DataFrame) -> DataFrame:
        """Seleciona e renomeia colunas para o contrato final do domínio.

        Args:
            df: DataFrame após aplicação do pipeline padrão.

        Returns:
            DataFrame: DataFrame no schema padronizado de saída.

        Notes:
            - Regra de negócio: ``tipo_chamada`` é forçado para string para
              uniformizar integração entre diferentes origens.
            - Anotação de manutenção: qualquer alteração de contrato de saída
              deve ocorrer neste método para preservar consistência.
        """
        return df.withColumn(
            "tipo_chamada", F.col("tipo_chamada").cast(T.StringType())
        ).select(
            F.col("referencia").alias("nu_referencia"),
            F.col("numero_origem").alias("nu_origem_original"),
            F.col("numero_destino").alias("nu_destino_original"),
            F.col("numero_origem_formatado").alias("nu_origem"),
            F.col("numero_origem_valido").alias("ic_origem_valido"),
            F.col("numero_destino_formatado").alias("nu_destino"),
            F.col("numero_destino_valido").alias("ic_destino_valido"),
            F.col("data_hora").alias("dh_chamada"),
            F.col("duracao").alias("qt_duracao_segundos"),
            F.col("tipo_chamada").alias("no_tipo_chamada"),
            F.col("autenticacao").alias("no_autenticacao"),
            F.col("prestadora").alias("no_prestadora"),
            F.col("tipo_cdr").alias("no_tipo_cdr"),
            F.col("arquivo_origem").alias("no_arquivo_origem"),
        )

    def _write_parquet(self, df: DataFrame, target_file: str) -> None:
        """Persiste o DataFrame transformado em parquet no destino informado.

        Args:
            df: DataFrame de entrada já transformado.
            target_file: Caminho de saída para gravação parquet.

        Returns:
            None: Método com efeito colateral de escrita em armazenamento.

        Notes:
            - A escrita usa ``overwrite`` para permitir reprocessamento idempotente.
            - O schema é padronizado imediatamente antes da gravação.
        """
        df = self._select_transformed_columns(df)
        df.repartition("no_tipo_chamada").write.mode("overwrite").partitionBy(
            "no_tipo_chamada"
        ).option("maxRecordsPerFile", MAX_RECORDS_PER_FILE).parquet(target_file)

    @log_operation
    def transform_cdr_ericsson(self, source_file: str, target_file: str):
        """Transforma CDR Ericsson extraído para o schema final padronizado.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Notes:
            O formato Ericsson utiliza máscara ``yyyy-MM-dd HH:mm:ss`` para
            parsing de data/hora.
        """

        date_time_fmt = "yyyy-MM-dd HH:mm:ss"
        df = self.spark.read.parquet(source_file)
        df = self._apply_standard_pipeline(df, date_time_fmt)
        df = df.withColumn(
            "tipo_chamada",
            F.when(F.col("_tipo_chamada") == "TER", "msTerminating")
            .when(F.col("_tipo_chamada") == "TRA", "transit")
            .when(F.col("_tipo_chamada") == "ORI", "msOriginating")
            .when(F.col("_tipo_chamada") == "ROA", "roamingCallForwarding")
            .when(F.col("_tipo_chamada") == "FOR", "callForwarding"),
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_claro_nokia(self, source_file: str, target_file: str):
        """Transforma CDR Claro Nokia extraído para o schema final padronizado.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Notes:
            O formato Claro Nokia utiliza máscara ``yyyy-MM-dd HH:mm:ss`` para
            parsing de data/hora.
        """

        date_time_fmt = "yyyy-MM-dd HH:mm:ss"
        df = self.spark.read.parquet(source_file)
        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_tim_ats(self, source_file: str, target_file: str):
        """Transforma CDR TIM ATS extraído para o schema final padronizado.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Notes:
            Utiliza formato temporal padrão do pipeline
            (``yyyy-MM-dd HH-mm-ss``), salvo ajuste explícito.
        """

        df = self.spark.read.parquet(source_file)
        df = self._apply_standard_pipeline(df)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_vivo_fcdr(self, source_file: str, target_file: str):
        """Transforma CDR Vivo FCDR extraído para o schema final padronizado.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Notes:
            - Regra de negócio específica: ``numero_origem`` pode vir composto
              com metadado de autenticação separado por ``;``.
            - A transformação extrai o número para ``numero_origem`` e move a
              segunda parte para ``_autenticacao``.
            - O formato temporal esperado neste layout é ``yyyyMMdd HHmmss``.
        """

        date_time_fmt = "yyyyMMdd HHmmss"
        df = self.spark.read.parquet(source_file)

        # Pré-processamento específico de layout: separa campo composto para
        # manter compatibilidade com o pipeline comum de autenticação.
        df = (
            df.withColumn("_split", F.split(F.col("_numero_origem"), ";"))
            .withColumn("numero_origem", F.col("_split").getItem(0))
            .withColumn("_autenticacao", F.col("_split").getItem(1))
            .drop("_split")
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
