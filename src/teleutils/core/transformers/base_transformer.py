"""Módulo base de transformação de CDR para o domínio padronizado.

Este módulo concentra operações compartilhadas de transformação de registros
de chamadas (CDR), independentemente da origem do layout bruto. O objetivo é
garantir consistência de schema, normalização de campos críticos e preparação
dos dados para consumo analítico.

Responsabilidades principais:
    - Padronizar data/hora e duração das chamadas.
    - Normalizar números de origem e destino com validação associada.
    - Derivar status de autenticação a partir de metadados de sinalização.
    - Consolidar o schema final e persistir o resultado em parquet.
    - Fornecer pré-processamentos específicos de layout quando necessário.

Principais funcionalidades:
    - Pipeline comum de transformação reutilizável.
    - Conversão defensiva de tipos para reduzir inconsistências entre fontes.
    - Seleção e renomeação para contrato final de dados do projeto.

Dependências relevantes:
    - pyspark.sql (DataFrame, funções e tipos)
    - teleutils._config.MIN_SAFE_DATE
    - teleutils.preprocessing.spark_normalize_number

Example:
    >>> transformer = CDRBaseTransformer(spark)
    >>> df_saida = transformer._apply_standard_pipeline(df_entrada)
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from teleutils._config import (
    MIN_SAFE_DATE,
    NULL_SENTINEL_VALUE,
    PRIMARY_KEY_COLUMNS,
    TARGET_SCHEMA,
)
from teleutils.preprocessing import spark_normalize_number

logger = logging.getLogger(__name__)


class CDRBaseTransformer:
    """Transformador base para normalização e padronização de CDRs.

    A classe encapsula regras comuns do domínio de telefonia que são aplicadas
    a diferentes layouts de entrada. Ela atua como camada de padronização antes
    da persistência dos dados em formato analítico.

    Contexto de uso:
        - Utilizada por transformadores específicos por prestadora/tipo de CDR.
        - Reaproveitada para evitar divergência de regra entre pipelines.

    Attributes:
        spark:
            Sessão Spark ativa utilizada para executar transformações
            distribuídas sobre DataFrames.

    Notes:
        Pontos de extensão devem priorizar métodos de pré-processamento por
        layout e manter o pipeline padrão centralizado neste componente.
    """

    def __init__(
        self,
        spark: SparkSession,
    ):
        """Inicializa o transformador com sessão Spark ativa.

        Args:
            spark: Sessão Spark compartilhada pelo pipeline de transformação.
        """

        self.spark = spark.newSession()
        self.spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")

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
            DataFrame: DataFrame com ``duracao`` normalizada e os campos
            temporais convertidos para timestamp.

        Notes:
            - Regra de negócio: duração inválida é tratada como 0 para manter
              consistência em métricas downstream.
            - As colunas ``duracao``, ``data_hora``, ``data_hora_fim`` e
              ``data_hora_referencia`` devem existir antes desta etapa.
            - Datas nulas, inválidas ou anteriores ao limite são normalizadas
              para ``MIN_SAFE_DATE``.
        """

        timestamp_format = F.lit(date_time_fmt)

        def normalize_timestamp(column_name):
            return F.greatest(
                F.try_to_timestamp(F.col(column_name), timestamp_format),
                MIN_SAFE_DATE,
            )

        return df.withColumns(
            {
                # Tratamento da duração (convertendo nulos e ausências para 0)
                "duracao": F.coalesce(
                    F.col("duracao").cast(T.IntegerType()),
                    F.lit(0).cast(T.IntegerType()),
                ),
                # Datas nulas, inválidas ou anteriores ao limite viram MIN_SAFE_DATE.
                "data_hora": normalize_timestamp("data_hora"),
                "data_hora_fim": normalize_timestamp("data_hora_fim"),
                "data_hora_referencia": normalize_timestamp("data_hora_referencia"),
            }
        )

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

        # formata números de origem e destino, adicionando colunas de validade
        df = (
            df.withColumn(
                "_numero_origem_formatado",
                spark_normalize_number("numero_origem"),  # type: ignore
            )
            .withColumn(
                "_numero_destino_formatado",
                spark_normalize_number("numero_destino"),  # type: ignore
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

        # se as colunas de origem/destino originais foram mantidas no dataframe original retorna ao dataframe final
        if "_numero_origem_original" in df.columns:
            df = df.withColumn("numero_origem", F.col("_numero_origem_original")).drop(
                "_numero_origem_original"
            )

        if "_numero_destino_original" in df.columns:
            df = df.withColumn(
                "numero_destino", F.col("_numero_destino_original")
            ).drop("_numero_destino_original")

        return df

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

    def _fill_missing_columns(self, df: DataFrame) -> DataFrame:
        """Adiciona ao DataFrame as colunas ausentes do contrato intermediário.

        Args:
            df: DataFrame Spark de entrada.

        Returns:
            DataFrame: DataFrame contendo todas as colunas de origem definidas
                em ``TARGET_SCHEMA``.

        Notes:
            As colunas ausentes são adicionadas como ``NULL`` sem conversão de
            tipo. Isso preserva valores textuais, especialmente timestamps,
            para que ``_format_date_time`` possa aplicar o formato recebido.
        """
        available_columns = set(df.columns)
        missing_columns = [
            source_column
            for source_column in TARGET_SCHEMA
            if source_column not in available_columns
        ]

        if missing_columns:
            logger.warning(
                "Colunas ausentes no DataFrame: %s. Criando-as como NULL.",
                missing_columns,
            )

        return df.withColumns(
            {source_column: F.lit(None) for source_column in missing_columns}
        )

    def _fill_primary_key_columns(self, df: DataFrame) -> DataFrame:
        """Preenche valores nulos das colunas que compõem a chave primária.

        Args:
            df: DataFrame após a normalização dos campos temporais e numéricos.

        Returns:
            DataFrame: DataFrame com as colunas da chave primária sem valores
            nulos.

        Notes:
            ``PRIMARY_KEY_COLUMNS`` usa os nomes finais das colunas, enquanto
            o DataFrame intermediário usa os nomes de origem definidos nas
            chaves de ``TARGET_SCHEMA``. O mapeamento entre esses nomes é feito
            nesta função.
        """
        primary_key_columns = {
            source_column: data_type
            for source_column, (target_column, data_type) in TARGET_SCHEMA.items()
            if target_column in PRIMARY_KEY_COLUMNS
        }
        columns_to_fill = {}

        for source_column, data_type in primary_key_columns.items():
            if isinstance(data_type, T.TimestampNTZType):
                default_value = MIN_SAFE_DATE
            elif isinstance(data_type, T.NumericType):
                default_value = F.lit(0).cast(data_type)
            else:
                default_value = NULL_SENTINEL_VALUE

            columns_to_fill[source_column] = F.coalesce(
                F.col(source_column).cast(data_type),
                default_value,
            )

        return df.withColumns(columns_to_fill)

    def _apply_standard_pipeline(
        self, df: DataFrame, date_time_fmt: str = "yyyy-MM-dd HH-mm-ss"
    ) -> DataFrame:
        """Executa pipeline comum de transformação para todos os layouts.

        Fluxo de processamento:
            1. Garantia da existência das colunas definidas em
               ``TARGET_SCHEMA``.
            2. Padronização temporal e duração.
            3. Garantia de valores não nulos nas colunas da chave primária.
            4. Normalização de números telefônicos.
            5. Enriquecimento de status de autenticação.

        Args:
            df: DataFrame de entrada.
            date_time_fmt: Formato de data/hora esperado para parsing.

        Returns:
            DataFrame: DataFrame transformado conforme regras padrão.

        Notes:
            Decisão arquitetural: centralizar o pipeline reduz risco de regras
            divergentes entre prestadoras e facilita manutenção evolutiva.

            As colunas do contrato são garantidas antes da normalização de
            números, que depende de ``numero_origem`` e ``numero_destino``.
        """

        df = self._fill_missing_columns(df)
        df = self._format_date_time(df, date_time_fmt)
        df = self._format_numbers(df)
        df = self._add_tn_validation_status(df)
        df = self._fill_primary_key_columns(df)

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

        return df.select(
            *[
                F.col(source_column).cast(data_type).alias(target_column)
                for source_column, (target_column, data_type) in TARGET_SCHEMA.items()
            ]
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
        logger.info("Escrevendo DataFrame transformado para parquet: %s", target_file)
        df = self._select_transformed_columns(df)
        df.write.mode("overwrite").partitionBy("no_tipo_chamada").parquet(target_file)
