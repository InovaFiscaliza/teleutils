"""Módulo base de transformação de CDR para o domínio padronizado.

Este módulo concentra operações compartilhadas de transformação de registros
de chamadas (CDR), independentemente da origem do layout bruto. O objetivo é
garantir consistência de schema, normalização de campos críticos e preparação
dos dados para consumo analítico.

Responsabilidades principais:
    - Padronizar data/hora e duração das chamadas.
    - Extrair números de origem e destino com indicador de sucesso.
    - Derivar status de autenticação a partir de metadados de sinalização.
    - Consolidar o schema final e persistir o resultado em parquet.
    - Garantir que os valores nulos das colunas da chave primária sejam
      substituídos por valores padrão.

Os pré-processamentos específicos de cada layout não fazem parte deste módulo;
eles são implementados pelas subclasses (por exemplo, ``CDRTransformer`` em
``teleutils.core.transformers.transformers``), que chamam
``_apply_standard_pipeline`` e ``_write_parquet``.

Fluxo geral (executado pelas subclasses):
    DataFrame pré-processado
      ↓ ``_apply_standard_pipeline``
    DataFrame com colunas de origem de ``TARGET_SCHEMA`` padronizadas
      ↓ ``_write_parquet`` (usa ``_select_transformed_columns``)
    Parquet final particionado por ``no_tipo_chamada``

Principais funcionalidades:
    - Pipeline comum de transformação reutilizável.
    - Conversão defensiva de tipos para reduzir inconsistências entre fontes.
    - Seleção e renomeação para contrato final de dados do projeto.

Dependências relevantes:
    - pyspark.sql (DataFrame, funções e tipos)
    - teleutils._config (``MIN_SAFE_DATE``, ``NULL_SENTINEL_VALUE``,
      ``PRIMARY_KEY_COLUMNS`` e ``TARGET_SCHEMA``)
    - teleutils.preprocessing.phone_number_extractor.extract_number

Example:
    >>> transformer = CDRBaseTransformer(spark)
    >>> df_saida = transformer._apply_standard_pipeline(df_entrada)
"""

from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf  # type: ignore

from teleutils._config import (
    MIN_SAFE_DATE,
    NULL_SENTINEL_VALUE,
    PRIMARY_KEY_COLUMNS,
    TARGET_SCHEMA,
)
from teleutils.preprocessing.phone_number_extractor import extract_number

logger = logging.getLogger(__name__)

# Schema Spark para o retorno da UDF pandas: (numero_extraido, extracao_sucesso).
# Mantido como constante de módulo para permitir reuso e evitar recriação por chamada.
_EXTRACTED_NUMBER_RETURN_SCHEMA = T.StructType(
    [
        T.StructField("numero_extraido", T.StringType(), True),
        T.StructField("extracao_sucesso", T.BooleanType(), True),
    ]
)


@pandas_udf(_EXTRACTED_NUMBER_RETURN_SCHEMA)  # type: ignore
def _extract_numbers(subscriber_number_series: pd.Series) -> pd.DataFrame:
    """Extrai números telefônicos em lote para uso em pipelines Spark.

    UDF pandas vetorizada que recebe uma série de números telefônicos brutos,
    aplica ``extract_number`` sobre cada elemento e retorna uma estrutura
    tabular com o número extraído e o indicador de sucesso.

    Args:
        subscriber_number_series: Série pandas contendo números em formato bruto,
            possivelmente com letras, pontuação e prefixos variados.

    Returns:
        pd.DataFrame: DataFrame com as seguintes colunas:
            - ``numero_extraido`` (str): Número extraído quando a extração
              tem sucesso. Quando falha, mantém o valor de entrada, exceto
              para entradas vazias/nulas, para as quais ``extract_number``
              retorna ``SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE``.
            - ``extracao_sucesso`` (bool): Indicador de sucesso da extração.

    Notes:
        A pandas UDF processa os dados por lote (batch), reduzindo o overhead de
        serialização em comparação com UDFs linha a linha, o que melhora
        significativamente o desempenho em grandes volumes de CDR.

        Anotação de manutenção: alterações no contrato de retorno (nomes ou
        tipos de coluna) exigem atualização coordenada de
        ``_EXTRACTED_NUMBER_RETURN_SCHEMA``, das chaves do ``pd.DataFrame``
        retornado e dos acessos ``numero_extraido`` e ``extracao_sucesso``
        feitos em ``CDRBaseTransformer._extract_phone_numbers``.
    """
    extraction_results = []
    for subscriber_number in subscriber_number_series:
        extraction_result = extract_number(subscriber_number=subscriber_number)
        extraction_results.append((extraction_result.number, extraction_result.success))

    return pd.DataFrame(
        extraction_results,
        columns=[
            "numero_extraido",
            "extracao_sucesso",
        ],
    )


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
            Nova sessão Spark derivada da sessão recebida, com
            ``spark.sql.timestampType`` definido como ``TIMESTAMP_NTZ``.
        min_safe_date:
            Expressão de coluna com ``MIN_SAFE_DATE`` convertida para
            ``TimestampNTZType``, usada como limite inferior e valor padrão
            para timestamps.
        null_sentinel_value:
            Expressão de coluna com ``NULL_SENTINEL_VALUE`` convertida para
            string, usada como valor padrão em colunas de chave primária não
            numéricas e não temporais.

    Notes:
        Pontos de extensão devem priorizar métodos de pré-processamento por
        layout e manter o pipeline padrão centralizado neste componente.

        As transformações de DataFrame são lazy; a ação de escrita em
        ``_write_parquet`` é a etapa deste módulo que materializa o plano Spark.
    """

    def __init__(
        self,
        spark: SparkSession,
    ):
        """Inicializa o transformador com sessão Spark ativa.

        Args:
            spark: Sessão Spark compartilhada pelo pipeline de transformação.

        Notes:
            Uma nova sessão Spark é criada a partir de ``spark`` e armazenada
            na instância. A configuração ``spark.sql.timestampType`` dessa
            sessão é definida como ``TIMESTAMP_NTZ`` antes das transformações
            temporais.
        """

        self.spark = spark.newSession()
        self.spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
        self.min_safe_date = F.lit(MIN_SAFE_DATE).cast(T.TimestampNTZType())
        self.null_sentinel_value = F.lit(NULL_SENTINEL_VALUE).cast(T.StringType())

    def _validate_timestamp_type(self) -> None:
        """Rejeita alterações incompatíveis na sessão gerenciada.

        Raises:
            ValueError: Se ``spark.sql.timestampType`` da sessão do
                transformador deixou de ser ``TIMESTAMP_NTZ``.

        Notes:
            Verificação executada por ``_resolve_input`` e ``_write_parquet``.
            Não restaura o valor: o chamador deve criar um novo transformador.
        """
        if self.spark.conf.get("spark.sql.timestampType") != "TIMESTAMP_NTZ":
            message = (
                "A sessão do transformador deve manter "
                "spark.sql.timestampType=TIMESTAMP_NTZ. "
                "Crie um novo transformador e reconstrua o DataFrame "
                "com um extrator usando transformer.spark."
            )
            logger.error(message)
            raise ValueError(message)

    def _resolve_input(self, source_file: str | DataFrame) -> DataFrame:
        """Lê Parquet ou aceita um DataFrame da própria sessão gerenciada.

        Sessões que compartilham apenas o SparkContext não são equivalentes.
        Nenhum plano é transferido e nenhuma configuração é corrigida
        silenciosamente. O chamador não deve alterar a sessão durante o uso.

        Args:
            source_file: Caminho Parquet, lido com ``self.spark``, ou DataFrame
                criado por essa mesma sessão (caminho direto, sem Parquet
                intermediário).

        Returns:
            DataFrame: Leitura do Parquet, ou o próprio DataFrame recebido.

        Raises:
            ValueError: Se a sessão deixou de usar ``TIMESTAMP_NTZ`` ou se o
                DataFrame pertence a outra sessão.
            TypeError: Se ``source_file`` não for ``str`` nem ``DataFrame``.
        """
        self._validate_timestamp_type()
        if isinstance(source_file, str):
            return self.spark.read.parquet(source_file)
        if not isinstance(source_file, DataFrame):
            message = "source_file deve ser um caminho str ou um DataFrame Spark."
            logger.error(message)
            raise TypeError(message)
        if source_file.sparkSession is not self.spark:
            message = (
                "O DataFrame deve pertencer à sessão do transformador. "
                "Crie o extrator com transformer.spark e reconstrua o DataFrame "
                "usando extract_dataframe; compartilhar o SparkContext não basta."
            )
            logger.error(message)
            raise ValueError(message)
        return source_file

    def _format_date_time(
        self,
        df,
        date_time_fmt: str = "yyyy-MM-dd HH-mm-ss",
        ref_date_time_fmt: str = "",
    ):
        """Padroniza campos temporais e normaliza duração.

        Objetivo da operação:
            Garantir que o dataset possua coluna ``data_hora`` em formato
            timestamp e que ``duracao`` esteja tipada como inteiro, com fallback
            para zero quando nula ou não conversível.

        Args:
            df: DataFrame Spark de entrada.
            date_time_fmt: Máscara de parsing para conversão de ``data_hora``
                e ``data_hora_fim``.
            ref_date_time_fmt: Máscara de parsing para
                ``data_hora_referencia``. Quando vazia, é utilizado
                ``date_time_fmt``.

        Returns:
            DataFrame: DataFrame com ``duracao`` normalizada e os campos
            temporais convertidos para timestamp.

        Notes:
            - Regra de negócio: duração nula ou não conversível é tratada como
              0 para manter consistência em métricas downstream.
              Anotação de manutenção: o fallback atua sobre o resultado nulo
              do ``cast``. Com ``spark.sql.ansi.enabled=true``, o cast de texto
              não numérico para inteiro levanta erro em vez de produzir nulo
              (confirmado com uma expressão isolada em PySpark 3.5.5).
            - As colunas ``duracao``, ``data_hora``, ``data_hora_fim`` e
              ``data_hora_referencia`` devem existir antes desta etapa.
            - ``try_to_timestamp`` produz nulo para valores não conversíveis;
              ``greatest`` ignora nulos e aplica ``MIN_SAFE_DATE`` como limite
              inferior, de modo que valores nulos, inválidos ou anteriores a
              esse limite resultam em ``MIN_SAFE_DATE``.
            - Regra de negócio: ``data_hora_fim`` informada é convertida com a
              mesma normalização; quando é nula, é calculada como
              ``data_hora`` somada a ``duracao`` (em segundos), já normalizadas.
        """

        timestamp_format = F.lit(date_time_fmt)
        if ref_date_time_fmt:
            ref_timestamp_format = F.lit(ref_date_time_fmt)
        else:
            ref_timestamp_format = timestamp_format

        def normalize_timestamp(column_name, timestamp_format):
            return F.greatest(
                F.try_to_timestamp(F.col(column_name), timestamp_format),
                self.min_safe_date,
            )

        def normalize_duration(column_name):
            return F.coalesce(
                F.col(column_name).cast(T.IntegerType()),
                F.lit(0).cast(T.IntegerType()),
            )

        df = df.withColumns(
            {
                # Converte duração nula ou não conversível para zero.
                "duracao": normalize_duration("duracao"),
                # Datas nulas, inválidas ou anteriores ao limite viram MIN_SAFE_DATE.
                "data_hora": normalize_timestamp("data_hora", timestamp_format),
                "data_hora_referencia": normalize_timestamp(
                    "data_hora_referencia", ref_timestamp_format
                ),
            }
        )

        return df.withColumn(
            "data_hora_fim",
            F.when(
                F.col("data_hora_fim").isNotNull(),
                normalize_timestamp("data_hora_fim", timestamp_format),
            ).otherwise(F.col("data_hora") + F.make_interval(secs=F.col("duracao"))),
        )

    def _extract_phone_numbers(self, df):
        """Extrai números de origem/destino e adiciona indicadores de sucesso.

        Args:
            df: DataFrame Spark contendo ao menos ``numero_origem`` e
                ``numero_destino``.

        Returns:
            DataFrame: DataFrame com ``numero_origem_extraido``,
            ``numero_destino_extraido``, ``sucesso_extracao_origem`` e
            ``sucesso_extracao_destino``. O valor de ``numero_*_extraido`` em
            caso de falha é definido por ``extract_number`` (ver
            ``_extract_numbers``).

        Notes:
            - A UDF retorna struct; por isso são criadas colunas temporárias
              intermediárias e depois expandidas.
            - Quando o pré-processamento fornece ``_numero_origem_original``
              e/ou ``_numero_destino_original``, esses valores brutos
              sobrescrevem ``numero_origem``/``numero_destino`` após a
              extração, e as colunas temporárias são removidas.
            - Efeito colateral lógico: colunas temporárias são removidas ao final
              para manter o schema limpo.
        """

        # A UDF produz structs temporários, expandidos nas colunas finais abaixo.
        df = (
            df.withColumn(
                "_extracao_numero_origem",
                _extract_numbers("numero_origem"),  # type: ignore
            )
            .withColumn(
                "_extracao_numero_destino",
                _extract_numbers("numero_destino"),  # type: ignore
            )
            .withColumn(
                "numero_origem_extraido",
                F.col("_extracao_numero_origem.numero_extraido"),
            )
            .withColumn(
                "sucesso_extracao_origem",
                F.col("_extracao_numero_origem.extracao_sucesso"),
            )
            .withColumn(
                "numero_destino_extraido",
                F.col("_extracao_numero_destino.numero_extraido"),
            )
            .withColumn(
                "sucesso_extracao_destino",
                F.col("_extracao_numero_destino.extracao_sucesso"),
            )
            .drop("_extracao_numero_origem")
            .drop("_extracao_numero_destino")
        )

        # Preserva os valores brutos quando o pré-processamento os disponibiliza.
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

            Anotação de manutenção: a mensagem de aviso registrada em log cita
            ``NULL_SENTINEL_VALUE`` como valor padrão, mas as colunas são
            efetivamente criadas com ``F.lit(None)`` (NULL).
        """
        available_columns = set(df.columns)
        missing_columns = [
            source_column
            for source_column in TARGET_SCHEMA
            if source_column not in available_columns
        ]

        if missing_columns:
            logger.warning(
                f"Colunas ausentes no DataFrame: %s. Criando-as com valor padrão {NULL_SENTINEL_VALUE}.",
                missing_columns,
            )

        return df.withColumns(
            {source_column: F.lit(None) for source_column in missing_columns}
        )

    def _fill_primary_key_columns(self, df: DataFrame) -> DataFrame:
        """Preenche valores nulos das colunas que compõem a chave primária.

        Args:
            df: DataFrame após as etapas anteriores do pipeline padrão.

        Returns:
            DataFrame: DataFrame com as colunas da chave primária sem valores
            nulos.

        Notes:
            ``PRIMARY_KEY_COLUMNS`` usa os nomes finais das colunas, enquanto
            o DataFrame intermediário usa os nomes de origem definidos nas
            chaves de ``TARGET_SCHEMA``. O mapeamento entre esses nomes é feito
            nesta função.

            Valores nulos são substituídos por ``MIN_SAFE_DATE`` para timestamps,
            por zero para tipos numéricos e por ``NULL_SENTINEL_VALUE`` para os
            demais tipos definidos na chave primária.

            Cada coluna da chave também é convertida (``cast``) para o tipo de
            ``TARGET_SCHEMA`` antes do preenchimento; o resultado substitui a
            coluna original. Um valor que o cast converte em nulo recebe o
            valor padrão, como um nulo de origem.
        """
        primary_key_columns = {
            source_column: data_type
            for source_column, (target_column, data_type) in TARGET_SCHEMA.items()
            if target_column in PRIMARY_KEY_COLUMNS
        }
        columns_to_fill = {}

        for source_column, data_type in primary_key_columns.items():
            if isinstance(data_type, T.TimestampNTZType):
                default_value = self.min_safe_date
            elif isinstance(data_type, T.NumericType):
                default_value = F.lit(0).cast(data_type)
            else:
                default_value = self.null_sentinel_value

            columns_to_fill[source_column] = F.coalesce(
                F.col(source_column).cast(data_type),
                default_value,
            )

        return df.withColumns(columns_to_fill)

    def _apply_standard_pipeline(
        self,
        df: DataFrame,
        date_time_fmt: str = "yyyy-MM-dd HH-mm-ss",
        ref_date_time_fmt="",
    ) -> DataFrame:
        """Executa pipeline comum de transformação para todos os layouts.

        Fluxo de processamento:
            1. Garantia da existência das colunas definidas em
               ``TARGET_SCHEMA``.
            2. Padronização temporal e duração.
            3. Extração de números telefônicos e indicadores de sucesso.
            4. Enriquecimento de status de autenticação.
            5. Garantia de valores não nulos nas colunas da chave primária.

        Args:
            df: DataFrame de entrada.
            date_time_fmt: Formato de data/hora esperado para parsing.
            ref_date_time_fmt: Formato de ``data_hora_referencia``; quando
                vazio, ``date_time_fmt`` é utilizado.

        Returns:
            DataFrame: DataFrame transformado conforme regras padrão, ainda
            com os nomes de origem das colunas; a seleção e a renomeação para
            o contrato final ocorrem em ``_write_parquet``.

        Notes:
            Decisão arquitetural: centralizar o pipeline reduz risco de regras
            divergentes entre prestadoras e facilita manutenção evolutiva.

            As colunas do contrato são garantidas antes da extração de
            números, que depende de ``numero_origem`` e ``numero_destino``.
        """

        df = self._fill_missing_columns(df)
        df = self._format_date_time(df, date_time_fmt, ref_date_time_fmt)
        df = self._extract_phone_numbers(df)
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
            - Regra de negócio: ``tipo_chamada`` é convertido para string
              (conforme ``TARGET_SCHEMA``) e renomeado para ``no_tipo_chamada``,
              uniformizando a integração entre diferentes origens.
            - Todas as colunas presentes em ``TARGET_SCHEMA`` são selecionadas,
              convertidas para o tipo configurado e renomeadas para o nome final
              do contrato. Colunas fora desse contrato não são incluídas.
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

        Raises:
            ValueError: Se a sessão do transformador deixou de usar
                ``TIMESTAMP_NTZ`` (verificado antes de qualquer escrita).

        Notes:
            - A escrita usa o modo ``overwrite``, que substitui o conteúdo
              existente em ``target_file``.
            - O schema é padronizado por ``_select_transformed_columns``
              imediatamente antes da gravação e a saída é particionada pela
              coluna final ``no_tipo_chamada``.
            - Esta é a ação Spark que materializa as transformações lazy.
        """
        self._validate_timestamp_type()
        logger.info("Escrevendo DataFrame transformado para parquet: %s", target_file)
        df = self._select_transformed_columns(df)
        df.write.mode("overwrite").partitionBy("no_tipo_chamada").parquet(target_file)
