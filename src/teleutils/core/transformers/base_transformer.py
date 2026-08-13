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

from teleutils._config import MIN_SAFE_DATE
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
                "data_hora",
                F.nullif(F.concat_ws(" ", F.col("_data"), F.col("_hora")), F.lit("")),
            )

        if "data_hora_fim" not in df.columns:
            df = df.withColumn(
                "data_hora_fim",
                F.nullif(
                    F.concat_ws(" ", F.col("_data"), F.col("_hora_fim")), F.lit("")
                ),
            )

        return df.withColumns(
            {
                # Tratamento da duração (convertendo nulos para 0)
                "duracao": F.coalesce(F.col("duracao").cast(T.IntegerType()), F.lit(0)),
                # 1. Faz o parse da string normalmente na CPU
                # 2. Se o resultado for uma data antiga (como ano 0000), força para NULL
                "data_hora": F.when(
                    F.try_to_timestamp(F.col("data_hora"), F.lit(date_time_fmt))
                    < MIN_SAFE_DATE,
                    None,
                ).otherwise(
                    F.try_to_timestamp(F.col("data_hora"), F.lit(date_time_fmt))
                ),
                "data_hora_fim": F.when(
                    F.try_to_timestamp(F.col("data_hora_fim"), F.lit(date_time_fmt))
                    < MIN_SAFE_DATE,
                    None,
                ).otherwise(
                    F.try_to_timestamp(F.col("data_hora_fim"), F.lit(date_time_fmt))
                ),
                "data_hora_referencia": F.when(
                    F.try_to_timestamp(F.col("data_hora_fim"), F.lit(date_time_fmt))
                    < MIN_SAFE_DATE,
                    None,
                ).otherwise(
                    F.try_to_timestamp(F.col("data_hora_fim"), F.lit(date_time_fmt))
                ),
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

        # Substitui caracteres '#' e '*' por 'c' e 'b', respectivamente para uniformizar a saída do Teleparser.
        df = df.withColumn(
            "numero_destino",
            F.regexp_replace(
                F.regexp_replace(F.col("numero_destino"), r"#", "c"), r"\*", "b"
            ),
        )

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
            # 1. Identificação Geral & Tempo (Quando e qual o contexto da carga)
            F.col("referencia").alias("nu_referencia"),
            F.col("data_hora_referencia").alias("dh_referencia"),
            F.col("data_hora").alias("dh_chamada"),
            F.col("data_hora_fim").alias("dh_fim_chamada"),
            F.col("duracao").alias("qt_duracao_segundos"),
            # 2. Partes Envolvidas (Quem ligou para quem)
            F.col("numero_origem_formatado").alias("nu_origem"),
            F.col("numero_origem_valido").alias("ic_origem_valido"),
            F.col("numero_origem").alias("nu_origem_original"),
            F.col("numero_destino_formatado").alias("nu_destino"),
            F.col("numero_destino_valido").alias("ic_destino_valido"),
            F.col("numero_destino").alias("nu_destino_original"),
            # 3. Status & Resultado da Chamada (O que aconteceu com a ligação)
            F.col("status_chamada").alias("no_resultado_chamada"),
            F.col("codigo_resposta_sip").alias("co_resposta_sip"),
            F.col("autenticacao").alias("no_autenticacao"),
            # 4. Roteamento & Rede Telecom (Por onde a chamada passou)
            F.col("prestadora").alias("no_prestadora"),
            F.col("rota_entrada").alias("no_rota_entrada"),
            F.col("rota_saida").alias("no_rota_saida"),
            F.col("bilhetador").alias("no_bilhetador"),
            # 5. Dados Técnicos de Dispositivo & IP (Células, aparelhos e IPs)
            F.col("celula_origem").alias("nu_cgi_origem"),
            F.col("imei_origem").alias("nu_imei_origem"),
            F.col("imsi_origem").alias("nu_imsi_origem"),
            F.col("ip_origem").alias("nu_ip_origem"),
            F.col("celula_destino").alias("nu_cgi_destino"),
            F.col("imei_destino").alias("nu_imei_destino"),
            F.col("imsi_destino").alias("nu_imsi_destino"),
            F.col("ip_destino").alias("nu_ip_destino"),
            F.col("agente_usuario").alias("no_agente_usuario"),
            # 6. Metadados do Arquivo & Regras de Negócio (Para auditoria e particionamento)
            F.col("tipo_cdr").alias("no_tipo_cdr"),
            F.col("arquivo_origem").alias("no_arquivo_origem"),
            F.col("tipo_chamada").alias("no_tipo_chamada"),
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

    def _preprocess_cdr_vivo_fcdr(self, df: DataFrame) -> DataFrame:
        """Aplica pré-processamento específico para layout FCDR da Vivo.

        Objetivo da operação:
            Extrair o token de autenticação embutido em ``_numero_origem`` e
            mapear códigos de ``_tipo_chamada`` para rótulos funcionais
            utilizados no restante do pipeline.

        Args:
            df: DataFrame bruto no layout FCDR da Vivo.

        Returns:
            DataFrame: DataFrame com ``numero_origem`` e ``_autenticacao``
            separados, além de ``tipo_chamada`` normalizado.

        Notes:
            - Regra de negócio: ``_numero_origem`` pode carregar metadados em
              formato "numero;autenticacao" e precisa ser decomposto.
            - O mapeamento de ``_tipo_chamada`` preserva valores não previstos,
              reduzindo risco de descarte de novas categorias enviadas pela
              origem.
            - Anotação de manutenção: novos códigos de ``_tipo_chamada`` devem
              ser adicionados na cadeia de ``when`` abaixo.
        """
        # Extrair autenticação e prefixos adicionais dos números.
        # A autenticação está contida na coluna _numero_origem,
        # por exemplo: 551136128860;verstat=TN-Validation-Passe
        df = (
            df.withColumn("_split", F.split(F.col("_numero_origem"), ";"))
            .withColumn("numero_origem", F.col("_split").getItem(0))
            .withColumn("_autenticacao", F.col("_split").getItem(1))
            .drop("_split")
            .withColumn(
                "tipo_chamada",
                F.when(F.col("_tipo_chamada") == "1", "msOriginating")
                .when(F.col("_tipo_chamada") == "3", "callForwarding")
                .when(F.col("_tipo_chamada") == "4", "msTerminating")
                .otherwise(F.col("_tipo_chamada")),
            )
        )

        return df
