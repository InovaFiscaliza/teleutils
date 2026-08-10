"""Módulo de transformações de CDRs extraídos via Teleparser.

Este módulo implementa transformadores específicos por fornecedor/layout de CDR
que reutilizam o pipeline comum definido no transformador base. O foco é
normalizar diferenças de schema e codificações de cada origem para produzir um
dataset analítico consistente.

Responsabilidades principais:
    - Ler CDRs intermediários em formato parquet.
    - Aplicar pré-processamentos por fornecedor quando necessário.
    - Acionar pipeline padrão de normalização temporal e telefônica.
    - Persistir resultado no contrato final de dados.

Principais funcionalidades:
    - Transformação para Ericsson.
    - Transformação para TIM/Huawei.
    - Transformação para Vivo/FCDR.
    - Transformação para Nokia.

Dependências relevantes:
    - pyspark.sql (SparkSession e funções colunares)
    - teleutils._logging.log_operation
    - teleutils.core.transformers.base_transformer.CDRBaseTransformer

Example:
    >>> transformer = CDRTeleparserTransformer(spark)
    >>> df = transformer.transform_cdr_nokia("/tmp/in", "/tmp/out")
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from teleutils._logging import log_operation
from teleutils.core.transformers.base_transformer import CDRBaseTransformer


class CDRTeleparserTransformer(CDRBaseTransformer):
    """Transformador de CDRs Teleparser com regras por fornecedor.

    A classe especializa o transformador base para lidar com peculiaridades de
    layouts de entrada processados pelo Teleparser. Cada método de
    transformação encapsula ajustes de campos que antecedem a execução do
    pipeline padrão.

    Contexto de uso:
        - Etapa de transformação após extração/parsing bruto dos CDRs.
        - Invocada por rotinas de ingestão para geração do dataset curado.

    Attributes:
        spark:
            Sessão Spark utilizada para leitura, transformação e escrita.

    Notes:
        Novos fornecedores devem ser adicionados como métodos dedicados,
        preservando o padrão de: leitura -> pré-processamento específico ->
        pipeline comum -> persistência.
    """

    def __init__(
        self,
        spark: SparkSession,
    ):
        """Inicializa o transformador com sessão Spark ativa.

        Args:
            spark: Sessão Spark compartilhada pelo pipeline de transformação.
        """

        super().__init__(spark)

    @log_operation
    def transform_cdr_ericsson(self, source_file: str, target_file: str):
        """Transforma CDR Ericsson para o contrato padronizado do domínio.

        Objetivo da operação:
            Converter a duração no formato ``HH:mm:ss`` para segundos inteiros
            e aplicar o pipeline padrão de normalização.

        Args:
            source_file: Caminho parquet com CDRs Ericsson de entrada.
            target_file: Caminho parquet de saída transformada.

        Returns:
            DataFrame: DataFrame lido do destino após escrita, já no padrão
            final utilizado no projeto.

        Notes:
            - Regra de negócio: duração ausente resulta em ``0``.
            - Efeito colateral: grava o resultado em ``target_file``.
            - Anotação de manutenção: se o formato de duração mudar na origem,
              este cálculo deve ser revisado antes do pipeline comum.
        """
        date_time_fmt = "yy-MM-dd HH:mm:ss"
        df = self.spark.read.parquet(source_file)

        col = F.col("duracao")
        # Decompõe HH:mm:ss em segundos totais para unificar a métrica de duração.
        hours = F.substring(col, 1, 2).cast("int") * 3600
        minutes = F.substring(col, 4, 2).cast("int") * 60
        seconds = F.substring(col, 7, 2).cast("int")
        df = df.withColumn(
            "duracao",
            F.when(col.isNotNull(), hours + minutes + seconds).otherwise(0).cast("int"),
        )
        df = self._apply_standard_pipeline(df, date_time_fmt)

        df = df.withColumns(
            {
                "celula_origem": F.concat_ws(
                    "-",
                    "`firstCallingLocationInformation.mcc`",
                    "`firstCallingLocationInformation.mnc`",
                    "`firstCallingLocationInformation.lac`",
                    F.lpad("`firstCallingLocationInformation.ci_sac`", 5, "0"),
                ),
                "celula_destino": F.concat_ws(
                    "-",
                    "`firstCalledLocationInformation.mcc`",
                    "`firstCalledLocationInformation.mnc`",
                    "`firstCalledLocationInformation.lac`",
                    F.lpad("`firstCalledLocationInformation.ci_sac`", 5, "0"),
                ),
                "imsi_origem": F.concat_ws(
                    "",
                    "`callingSubscriberIMSI.mcc`",
                    "`callingSubscriberIMSI.mnc`",
                    "`callingSubscriberIMSI.msin`",
                ),
                "imsi_destino": F.concat_ws(
                    "",
                    "`calledSubscriberIMSI.mcc`",
                    "`calledSubscriberIMSI.mnc`",
                    "`calledSubscriberIMSI.msin`",
                ),
                "imei_origem": F.concat_ws(
                    "",
                    "`callingSubscriberIMEI.type_allocation_code`",
                    "`callingSubscriberIMEI.serial_number`",
                ),
                "imei_destino": F.concat_ws(
                    "",
                    "`calledSubscriberIMEI.type_allocation_code`",
                    "`calledSubscriberIMEI.serial_number`",
                ),
            }
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_tim_huawei(self, source_file: str, target_file: str):
        """Transforma CDR TIM/Huawei para o contrato padronizado do domínio.

        Objetivo da operação:
            Aplicar filtros de qualidade mínima, extrair autenticação de campo
            genérico e remover prefixos não discáveis dos números antes da
            normalização central.

        Args:
            source_file: Caminho parquet com CDRs TIM/Huawei de entrada.
            target_file: Caminho parquet de saída transformada.

        Returns:
            DataFrame: DataFrame lido do destino após escrita, no schema final.

        Notes:
            - Regra de negócio: registros sem ``referencia`` são descartados por
              não atenderem aos critérios mínimos analíticos.
            - Efeito colateral: grava o resultado em ``target_file``.
            - Anotação de manutenção: a regra de remoção de prefixo pressupõe
              metadados fixos de 2 caracteres no início do número.
        """
        date_time_fmt = "yyyy-MM-dd HH:mm:ssxxx"
        df = self.spark.read.parquet(source_file)

        df = (
            # Excluir registros com referência nula, pois não são válidos para análise.
            df.filter(F.col("referencia").isNotNull())
            # Extrair autenticação e prefixos adicionais dos números.
            # A autenticação está contida na coluna _numero_origem_generico,
            # por exemplo: verstat=TN-Validation-Passed
            .withColumn(
                "_autenticacao",
                F.regexp_extract(
                    "_numero_origem_generico", r"(verstat=[a-zA-Z\-]+)", 0
                ),
            )
            # Remover caracteres adicionais dos números de telefone, mantendo apenas os 20  caracteres.
            # As colunas numero_origem e numero_destino contêm os números dos terminais
            # precedidos de prefixos adicionais (11 ou 14) que devem ser removidos:
            #
            # +-----------------|---------------+
            # | Antes           | Depois        |
            # |-----------------|---------------|
            # | 1440042704      | 40042704      |
            # | 115595981241366 | 5595981241366 |
            # | 1408000910091   | 08000910091   |
            # +-----------------|---------------+
            .withColumn(
                "numero_origem", F.col("numero_origem").substr(3, 9999)
            )  # spark exige o terceiro argumento para substr, mesmo que seja maior que o tamanho da string
            .withColumn("numero_destino", F.col("numero_destino").substr(3, 9999))
        )
        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_vivo_fcdr(self, source_file: str, target_file: str):
        """Transforma CDR Vivo/FCDR para o contrato padronizado do domínio.

        Objetivo da operação:
            Executar o pré-processamento específico da Vivo/FCDR para separar
            metadados embutidos e, em seguida, aplicar a normalização padrão.

        Args:
            source_file: Caminho parquet com CDRs Vivo/FCDR de entrada.
            target_file: Caminho parquet de saída transformada.

        Returns:
            DataFrame: DataFrame lido do destino após escrita, no schema final.

        Notes:
            - Integração relevante: utiliza método especializado herdado do
              transformador base para preparar campos da Vivo.
            - Efeito colateral: grava o resultado em ``target_file``.
        """
        date_time_fmt = "yyyyMMdd HHmmss"
        df = self.spark.read.parquet(source_file)
        df = self._preprocess_cdr_vivo_fcdr(df)
        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_nokia(self, source_file: str, target_file: str):
        """Transforma CDR Nokia para o contrato padronizado do domínio.

        Objetivo da operação:
            Consolidar campos variantes de duração/data e ajustar números para
            cenários de encaminhamento antes do pipeline padrão.

        Args:
            source_file: Caminho parquet com CDRs Nokia de entrada.
            target_file: Caminho parquet de saída transformada.

        Returns:
            DataFrame: DataFrame lido do destino após escrita, no schema final.

        Notes:
            - Regra de negócio: múltiplos campos ``_duracao*`` são reduzidos a
              uma única duração por registro via ``coalesce``.
            - Regra de negócio: para chamadas ``FORW``, o destino é derivado do
              campo de encaminhamento para melhor aderência semântica.
            - Efeito colateral: grava o resultado em ``target_file``.
            - Anotação de manutenção: divergências residuais com parser legado
              devem ser monitoradas em homologações futuras.
        """
        date_time_fmt = "dd/MM/yyyy HH:mm:ss"
        df = self.spark.read.parquet(source_file)

        # CDRs Nokia possuem um campo de duração específico para cada tipo, apenas um com valor não nulo por registro.
        # A expressão a seguir garante apenas uma coluna com duracao final preenchida com o valor correto, independentemente do tipo de CDR.
        duration_columns = [col for col in df.columns if col.startswith("_duracao")]
        coalesce_duration_expression = [F.col(c) for c in duration_columns]
        coalesce_duration_expression.append(F.lit("0"))
        coalesce_duration_expression = F.coalesce(*coalesce_duration_expression)
        df = df.withColumn("duracao", coalesce_duration_expression)

        # Alguns CDRs não contém valor em data_hora_alocacao_canal, mas possuem data_hora_referencia preenchida.
        # A expressão a seguir garante que a coluna data_hora final seja preenchida, ainda que por nulo, independentemente do tipo de CDR.
        df = df.withColumn(
            "data_hora",
            F.coalesce(
                F.col("data_hora_alocacao_canal"), F.col("data_hora_referencia")
            ),
        )

        # CDRs do tipo FORW não possuem os campos calling_number e called_number
        # considerar os campos alternativos mapeados no extrator:
        #
        # +-------------------------+-------------------------------+
        # | Antes (CDR Bruto)       | Depois (CDR Extraído)         |
        # |-------------------------|-------------------------------|
        # | calling_number          | _numero_origem                |
        # | orig_calling_number     | numero_origem_original        |
        # | called_number           | _numero_destino               |
        # | orig_called_number      | numero_destino_original       |
        # | forwarding_number       | numero_origem_encaminhamento  |
        # | forwarded_to_number     | numero_destino_encaminhamento |
        # +------------------------+-------------------------------+
        #
        # O script legado utiliza as colunas `orig_calling_number` e `forwarding_number` para derivar os campos numero_origem e numero_destino.
        # Os campos utilizados por esse script (numero_origem_original e numero_destino_original) mostraram o mesmo resultados e estão mais aderentes à documentação Nokia, com erro muito pequeno em relação ao parser legado.
        # Validar se quando a chamada é encaminhada para a caixa postal ocorre que o número de destino encaminhar para si mesmo a chamada.
        # Em exemplo analisado onde cause_for_forwarding = `SCP initiated`, forwarding_number = `8885561993363275` e forwared_to_number = `C145561993363275`, o que pode indicar que a chamada foi encaminhada para a caixa postal do próprio número de destino.
        df = df.withColumn(
            "numero_origem",
            F.coalesce(F.col("numero_origem"), F.col("numero_origem_original")),
        ).withColumn(
            "numero_destino",
            F.when(
                F.col("tipo_chamada") == "FORW", F.col("numero_origem_encaminhamento")
            ).otherwise(
                F.col("numero_destino"),
            ),
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
