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

from functools import reduce
from operator import or_

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from teleutils._config import ALGAR_MNC, CLARO_MNC, DEFAULT_MCC, MIN_SAFE_DATE
from teleutils._logging import log_operation
from teleutils.core.transformers.base_transformer import CDRBaseTransformer


def _null_if_blank(column_name: str):
    column = F.col(column_name)
    return F.when(
        F.trim(column.cast("string")) == "",
        F.lit(None),
    ).otherwise(column)


def _concat_or_null(separator: str, *columns):
    # 1. Normaliza os argumentos garantindo objetos Column
    cols = [F.col(c) if isinstance(c, str) else c for c in columns]

    # 2. Retorna True se QUALQUER coluna for NULL
    has_any_null = reduce(or_, [c.isNull() for c in cols])

    # 3. Retorna NULL se houver algum nulo, caso contrário, executa o concat_ws
    return F.when(has_any_null, F.lit(None)).otherwise(F.concat_ws(separator, *cols))


def _build_composite_column(
    separator: str,
    components: tuple,
    padded_columns: tuple[str, ...] = (),
):
    columns = []
    for component in components:
        column = _null_if_blank(component) if isinstance(component, str) else component
        if isinstance(component, str) and component in padded_columns:
            column = F.lpad(column, 5, "0")
        columns.append(column)

    return _concat_or_null(separator, *columns)


def _format_cell_id(df, col_name, out_col, gnb_id_bits=26):
    c = F.col(col_name)
    length = F.length(c)

    # ---- 3G (UTRAN, 13 chars) ----
    tac_3g = F.conv(F.substring(col_name, 6, 4), 16, 10).cast("long")
    ci_3g = F.conv(F.substring(col_name, 10, 4), 16, 10).cast("long")
    ci_formatted = F.concat_ws(
        "-",
        F.substring(col_name, 1, 3),  # mcc
        F.substring(col_name, 4, 2),  # mnc
        F.lpad(tac_3g.cast("string"), 5, "0"),  # tac (16 bits)
        F.lpad(ci_3g.cast("string"), 5, "0"),  # ci (16 bits)
    )

    # ---- 4G (ECGI, 16 chars) ----
    ecgi_val = F.conv(F.substring(col_name, 10, 7), 16, 10).cast("long")
    ecgi_formatted = F.concat_ws(
        "-",
        F.substring(col_name, 1, 3),  # mcc
        F.substring(col_name, 4, 2),  # mnc
        F.lpad(
            (ecgi_val / 256).cast("long").cast("string"), 7, "0"
        ),  # enb_id (20 bits)
        F.lpad((ecgi_val % 256).cast("string"), 3, "0"),  # cell_id (8 bits)
    )

    # ---- 5G (NCGI, 20 chars) ----
    # NCGI = 36 bits totais. gNB ID = 26 bits (default), Cell ID = 36 - 26 = 10 bits
    cell_id_bits = 36 - gnb_id_bits  # 10
    cell_id_mask = (1 << cell_id_bits) - 1  # 0x3FF = 1023

    ncgi_val = F.conv(F.substring(col_name, 12, 9), 16, 10).cast("long")
    ncgi_formatted = F.concat_ws(
        "-",
        F.substring(col_name, 1, 3),  # mcc
        F.substring(col_name, 4, 2),  # mnc
        F.lpad(
            F.shiftright(ncgi_val, cell_id_bits).cast("string"), 8, "0"
        ),  # gnb_id (26 bits)
        F.lpad(
            ncgi_val.bitwiseAND(cell_id_mask).cast("string"), 4, "0"
        ),  # cell_id (10 bits)
    )

    return df.withColumn(
        out_col,
        F.when(length == 13, ci_formatted)
        .when(length == 16, ecgi_formatted)
        .when(length == 20, ncgi_formatted)
        .otherwise(F.col(col_name)),
    )


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

        df = df.withColumns(
            {
                "celula_origem": _build_composite_column(
                    "-",
                    (
                        "celula_origem_mcc",
                        "celula_origem_mnc",
                        "celula_origem_lac",
                        "celula_origem_ci_sac",
                    ),
                    ("celula_origem_lac", "celula_origem_ci_sac"),
                ),
                "celula_destino": _build_composite_column(
                    "-",
                    (
                        "celula_destino_mcc",
                        "celula_destino_mnc",
                        "celula_destino_lac",
                        "celula_destino_ci_sac",
                    ),
                    ("celula_destino_lac", "celula_destino_ci_sac"),
                ),
                "imsi_origem": _build_composite_column(
                    "",
                    ("imsi_origem_mcc", "imsi_origem_mnc", "imsi_origem_msin"),
                ),
                "imsi_destino": _build_composite_column(
                    "",
                    ("imsi_destino_mcc", "imsi_destino_mnc", "imsi_destino_msin"),
                ),
                "imei_origem": _build_composite_column(
                    "", ("imei_origem_tac", "imei_origem_sn")
                ),
                "imei_destino": _build_composite_column(
                    "", ("imei_destino_tac", "imei_destino_sn")
                ),
            }
        )

        # Colunas inexistentes nos CDR Ericsson, mas exigidas pelo contrato final, são preenchidas com nulo.
        missing_ts_columns = ["data_hora_referencia"]
        missing_string_columns = ["ip_origem", "ip_destino", "agente_usuario"]
        missing_int_columns = [
            "porta_ip_origem",
            "porta_ip_destino",
            "codigo_resposta_sip",
        ]
        missing_columns = {
            **{col: MIN_SAFE_DATE for col in missing_ts_columns},
            **{col: F.lit(None).cast(T.StringType()) for col in missing_string_columns},
            **{col: F.lit(None).cast(T.IntegerType()) for col in missing_int_columns},
        }
        df = df.withColumns(missing_columns)

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_lte_huawei_tim(self, source_file: str, target_file: str):
        """Transforma CDR TIM LTE Huawei para o contrato padronizado do domínio.

        Objetivo da operação:
            Aplicar filtros de qualidade mínima, extrair autenticação de campo
            genérico e remover prefixos não discáveis dos números antes da
            normalização central.

        Args:
            source_file: Caminho parquet com CDRs TIM LTE Huawei de entrada.
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
        date_time_fmt = "yyyy-MM-dd HH:mm:ssXXX"
        df = self.spark.read.parquet(source_file)

        is_ats = F.col("tipo_cdr") == "aTSRecord"
        is_ibcf = F.col("tipo_cdr") == "iBCFRecord"
        is_originating = F.col("tipo_chamada") == "oRIGINATING-ROLE"
        is_terminating = F.col("tipo_chamada") == "tERMINATING-ROLE"
        

        df = (
            df.withColumn(
                "_autenticacao",
                F.when(
                    is_ats,
                    F.regexp_extract(
                        F.col("_numero_origem_ats_auth"), r"(verstat=[a-zA-Z\-]+)", 0
                    ),
                ).otherwise(
                    F.regexp_extract(
                        F.col("_numero_origem_ibcf"), r"(verstat=[a-zA-Z\-]+)", 0
                    )
                ),
            )
            .withColumn(
                "numero_origem",
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
                F.when(is_ats, F.col("_numero_origem_ats").substr(3, 9999)).otherwise(
                    F.regexp_extract(
                        F.col("_numero_origem_ibcf"), r"sip:([0-9]+)[@;]", 1
                    )
                ),
            )
            .withColumn(
                "numero_destino",
                F.when(is_ats, F.col("_numero_destino_ats").substr(3, 9999)).otherwise(
                    F.regexp_extract(
                        F.col("_numero_destino_ibcf"), r"sip:([0-9]+)[@;]", 1
                    )
                ),
            )
            .withColumn(
                "referencia",
                F.when(F.col("referencia").isNull(), F.lit("FFFFFFFFFF")).otherwise(
                    F.col("referencia")
                ),
            )
        )

        df = df.withColumn(
            "_cell_id",
            F.regexp_extract(
                "_informacao_rede", r"utran-cell-id-3gpp=([0-9a-zA-Z]+);", 1
            ),
        )
        df = _format_cell_id(df, "_cell_id", "_cell_id")

        df = df.withColumn(
            "_imei",
            F.when(F.col("_info_imei") == "iMEI", F.col("_imei")).otherwise(
                F.lit(None)
            ),
        )
        df = df.withColumn(
            "_imsi",
            F.when(F.col("_info_imsi") == "eND-USER-IMSI", F.col("_imsi")).otherwise(
                F.lit(None)
            ),
        )

        df = df.withColumns(
            {
                "celula_origem": F.when(is_originating, F.col("_cell_id")),
                "celula_destino": F.when(is_terminating, F.col("_cell_id")),
                "imei_origem": F.when(is_originating, F.col("_imei")),
                "imei_destino": F.when(is_terminating, F.col("_imei")),
                "imsi_origem": F.when(is_originating, F.col("_imsi")),
                "imsi_destino": F.when(is_terminating, F.col("_imsi")),
            },
        )

        df = df.withColumn(
            "_status_chamada",
            F.when(
                is_ibcf,
                F.regexp_extract(
                    F.col("_status_chamada"), r"SIP;cause=([0-9]+);", 1
                ).cast(T.IntegerType()),
            ).otherwise(F.col("_status_chamada").cast(T.IntegerType())),
        ).withColumn(
            "codigo_resposta_sip",
            F.when(
                F.col("_status_chamada")
                >= 200,  # códigos de resposta SIP válidos são >= 200
                F.col("_status_chamada"),
            ).otherwise(F.lit(None).cast(T.IntegerType())),
        )

        df = df.withColumn(
            "status_chamada",
            F.when(
                (F.col("_status_chamada") <= -300) & (F.col("_status_chamada") > -400),
                F.lit("Redirection"),
            )
            .when(
                (F.col("_status_chamada") <= -200) & (F.col("_status_chamada") > -300),
                F.lit("Final Response"),
            )
            .when(F.col("_status_chamada") == -3, F.lit("End of REGISTER dialog"))
            .when(F.col("_status_chamada") == -2, F.lit("End of SUBSCRIBE dialog"))
            .when(F.col("_status_chamada") == -1, F.lit("Successful transaction"))
            .when(F.col("_status_chamada") == 0, F.lit("Normal end of session"))
            .when(F.col("_status_chamada") == 1, F.lit("Unspecified error"))
            .when(F.col("_status_chamada") == 2, F.lit("Unsuccessful session setup"))
            .when(F.col("_status_chamada") == 3, F.lit("Internal error"))
            .when(F.col("_status_chamada") == 4, F.lit("Session timer timeout"))
            .when(F.col("_status_chamada") == 5, F.lit("CAC_REJECT"))
            .when(F.col("_status_chamada") == 200, F.lit("Normal end of session"))
            .when(
                (F.col("_status_chamada") > 200) & (F.col("_status_chamada") < 300),
                F.lit("Final Response"),
            )
            .when(
                (F.col("_status_chamada") >= 300) & (F.col("_status_chamada") < 400),
                F.lit("Redirection"),
            )
            .when(
                (F.col("_status_chamada") >= 400) & (F.col("_status_chamada") < 500),
                F.lit("Request failure"),
            )
            .when(
                (F.col("_status_chamada") >= 500) & (F.col("_status_chamada") < 600),
                F.lit("Server failure"),
            )
            .when(
                (F.col("_status_chamada") >= 600) & (F.col("_status_chamada") < 700),
                F.lit("Global failure"),
            )
            .otherwise(F.lit(None).cast(T.StringType())),
        )

        # Colunas inexistentes nos CDR Tim Huawei, mas exigidas pelo contrato final, são preenchidas com nulo.
        missing_ts_columns = ["data_hora_referencia"]
        missing_string_columns = ["ip_origem", "ip_destino"]
        missing_int_columns = [
            "porta_ip_origem",
            "porta_ip_destino",
        ]
        missing_columns = {
            **{col: MIN_SAFE_DATE for col in missing_ts_columns},
            **{col: F.lit(None).cast(T.StringType()) for col in missing_string_columns},
            **{col: F.lit(None).cast(T.IntegerType()) for col in missing_int_columns},
        }
        df = df.withColumns(missing_columns)

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_lte_ericsson_vivo(self, source_file: str, target_file: str):
        """Transforma CDR Vivo LTE Ericsson para o contrato padronizado do domínio.

        Objetivo da operação:
            Executar o pré-processamento específico da Vivo LTE Ericsson para separar
            metadados embutidos e, em seguida, aplicar a normalização padrão.

        Args:
            source_file: Caminho parquet com CDRs Vivo LTE Ericsson de entrada.
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
            .withColumn(
                "status_chamada",
                F.when(
                    F.col("_status_chamada") == "1",
                    "callHasReachedCongestionOrBusyState",
                )
                .when(
                    F.col("_status_chamada") == "2",
                    "callHasOnlyReachedThroughConnection",
                )
                .when(F.col("_status_chamada") == "3", "b-AnswerHasBeenReceived")
                .otherwise(F.col("_status_chamada")),
            )
            .withColumns(
                {
                    "imei_origem": F.translate(F.col("imei_origem"), "-", ""),
                    "imei_destino": F.translate(F.col("imei_destino"), "-", ""),
                }
            )
        )

        df = _format_cell_id(df, "celula_origem", "celula_origem")
        df = _format_cell_id(df, "celula_destino", "celula_destino")

        # Colunas inexistentes nos CDR Vivo Huawei, mas exigidas pelo contrato final, são preenchidas com nulo.
        missing_ts_columns = ["data_hora_referencia"]
        missing_string_columns = ["ip_origem", "ip_destino", "agente_usuario"]
        missing_int_columns = [
            "porta_ip_origem",
            "porta_ip_destino",
            "codigo_resposta_sip",
        ]
        missing_columns = {
            **{col: MIN_SAFE_DATE for col in missing_ts_columns},
            **{col: F.lit(None).cast(T.StringType()) for col in missing_string_columns},
            **{col: F.lit(None).cast(T.IntegerType()) for col in missing_int_columns},
        }
        df = df.withColumns(missing_columns)

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

        # Trata o valor sentinela "FFFFFF", presentes em CDRs Algar, em cada coluna de origem, transformando em nulo,
        # para que o coalesce já ignore esses valores automaticamente.
        cleaned_duration_cols = [
            F.when(F.col(c) == "FFFFFF", F.lit(None)).otherwise(F.col(c))
            for c in duration_columns
        ]
        df = df.withColumn("duracao", F.coalesce(*cleaned_duration_cols))

        # Alguns CDRs não contém valor em data_hora_alocacao_canal, mas possuem data_hora_referencia preenchida.
        # A expressão a seguir garante que a coluna data_hora final seja preenchida, ainda que por nulo, independentemente do tipo de CDR.
        # Se nenhuma das datas existir, preenche com valor sentinela MIN_SAFE_DATE para evitar nulos em campo crítico.
        df = df.withColumn(
            "data_hora",
            F.coalesce(
                F.col("data_hora_alocacao_canal"),
                F.col("data_hora_referencia"),
                MIN_SAFE_DATE,
            ),
        )
        # A coluna data_hora_fim é derivada de forma condicional, considerando o tipo de CDR.
        # Para CDRs do tipo UCA, a data_hora_fim é obtida a partir de data_hora_desconexao, caso exista.
        # Se nenhuma das datas existir, preenche com valor sentinela MIN_SAFE_DATE para evitar nulos em campo crítico.
        if "data_hora_desconexao" in df.columns:
            df = df.withColumn(
                "data_hora_fim",
                F.coalesce(
                    F.when(
                        F.col("tipo_chamada") == "UCA", F.col("data_hora_desconexao")
                    ).otherwise(F.col("data_hora_fim")),
                    MIN_SAFE_DATE,
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
        # +------------------------+--------------------------------+
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

        # CDRs Nokia não possuem campos de MCC/MNC; o MNC é imputado pela prestadora.
        nokia_mnc = F.when(F.col("prestadora") == "claro", CLARO_MNC).when(
            F.col("prestadora") == "algar", ALGAR_MNC
        )
        df = (
            df.withColumn("_nokia_mnc", nokia_mnc)
            .withColumns(
                {
                    "celula_origem": _build_composite_column(
                        "-",
                        (
                            DEFAULT_MCC,
                            F.col("_nokia_mnc"),
                            "celula_origem_lac",
                            "celula_origem_ci",
                        ),
                        ("celula_origem_lac", "celula_origem_ci"),
                    ),
                    "celula_destino": _build_composite_column(
                        "-",
                        (
                            DEFAULT_MCC,
                            F.col("_nokia_mnc"),
                            "celula_destino_lac",
                            "celula_destino_ci",
                        ),
                        ("celula_destino_lac", "celula_destino_ci"),
                    ),
                }
            )
            .drop("_nokia_mnc")
        )

        # Agrupar os valores de _status_chamada em faixas de códigos de status, conforme documentação Nokia:
        # +-----------------+---------------------+
        # | _status_chamada | descrição           |
        # +-----------------+---------------------+
        # | 0000H - 03FFH   | normal clearing     |
        # | 0400H - 07FFH   | internal congestion |
        # | 0800H - 0BFFH   | external congestion |
        # | 0C00H - 0FFFH   | subscriber errors   |
        # | 1000H -         | event codes         |
        # +-----------------+---------------------+
        df = df.withColumn(
            "status_chamada",
            F.when(
                (F.col("_status_chamada") >= F.lit(int("0000", 16)))
                & (F.col("_status_chamada") <= F.lit(int("03FF", 16))),
                F.lit("normal clearing"),
            )
            .when(
                (F.col("_status_chamada") >= F.lit(int("0400", 16)))
                & (F.col("_status_chamada") <= F.lit(int("07FF", 16))),
                F.lit("internal congestion"),
            )
            .when(
                (F.col("_status_chamada") >= F.lit(int("0800", 16)))
                & (F.col("_status_chamada") <= F.lit(int("0BFF", 16))),
                F.lit("external congestion"),
            )
            .when(
                (F.col("_status_chamada") >= F.lit(int("0C00", 16)))
                & (F.col("_status_chamada") <= F.lit(int("0FFF", 16))),
                F.lit("subscriber errors"),
            )
            .when(
                F.col("_status_chamada") >= F.lit(int("1000", 16)), F.lit("event codes")
            )
            .otherwise(F.lit(None)),
        )

        # Colunas inexistentes nos CDR Nokia, mas exigidas pelo contrato final, são preenchidas com nulo.
        missing_string_columns = ["ip_origem", "ip_destino", "agente_usuario"]
        missing_int_columns = [
            "porta_ip_origem",
            "porta_ip_destino",
            "codigo_resposta_sip",
        ]
        missing_columns = {
            **{col: F.lit(None).cast(T.StringType()) for col in missing_string_columns},
            **{col: F.lit(None).cast(T.IntegerType()) for col in missing_int_columns},
        }
        df = df.withColumns(missing_columns)

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
