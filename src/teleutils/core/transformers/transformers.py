"""Módulo de transformações de CDRs intermediários.

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
    >>> transformer = CDRTransformer(spark)
    >>> df = transformer.transform_cdr_nokia("/tmp/in", "/tmp/out")
"""

from __future__ import annotations

from functools import reduce
from operator import or_

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from teleutils._config import ALGAR_MNC, CLARO_MNC, DEFAULT_MCC
from teleutils._logging import log_operation
from teleutils.core.transformers.base_transformer import CDRBaseTransformer

# Regex utilizado para extrair o marcador de autenticação (ex.: "verstat=TN-Validation-Passed")
# embutido em campos SIP de origem de CDRs Huawei.
_AUTH_EXTRACT_PATTERN = r"(verstat=[a-zA-Z\-]+)"

# Regex utilizado para extrair o identificador de célula 3GPP embutido em campos de rede de CDRs.
# Exemplo: 3GPP-E-UTRAN-FDD;utran-cell-id-3gpp=7240295068176515;network-provided
_CELL_EXTRACT_PATTERN = r"3gpp=([0-9a-fA-F]+);?"

# Formatos de saída válidos para transformações de CDRs.
_VALID_OUTPUT_FORMATS = {"default", "tim"}


def _check_output_format(
    output_format: str, valid_output_formats: set[str] = _VALID_OUTPUT_FORMATS
):
    """Verifica se o formato de saída fornecido é válido.

    Args:
        output_format: Formato de saída a ser verificado.
        valid_output_formats: Conjunto de formatos de saída válidos.

    Raises:
        ValueError: Se o formato de saída não estiver no conjunto de válidos.
    """
    if output_format not in valid_output_formats:
        raise ValueError(
            f"Formato de saída inválido: {output_format}. Formatos válidos: {valid_output_formats}"
        )


def _null_if_blank(column_name: str):
    """Converte valores de string vazios/em branco em nulo Spark.

    Objetivo da operação:
        Uniformizar campos de origem que podem chegar como string vazia em vez
        de nulo, evitando que esses valores poluam concatenações posteriores
        (ex.: chaves compostas montadas por ``_concat_or_null``).

    Args:
        column_name: Nome da coluna a ser avaliada e normalizada.

    Returns:
        Column: Expressão Spark que resulta no valor original da coluna, ou
            ``NULL`` quando o conteúdo (após ``trim``) for uma string vazia.
    """
    column = F.col(column_name)
    return F.when(
        F.trim(column.cast("string")) == "",
        F.lit(None),
    ).otherwise(column)


def _concat_or_null(separator: str, *columns):
    """Concatena colunas com separador, retornando nulo se qualquer uma for nula.

    Objetivo da operação:
        Evitar a formação de identificadores compostos parcialmente
        preenchidos (ex.: ``"216-XX--"``), o que mascararia a ausência de
        dados essenciais para correlação de registros.

    Args:
        separator: Separador utilizado entre os componentes concatenados.
        *columns: Colunas (``Column`` ou nome de coluna em ``str``) a
            concatenar, na ordem em que devem aparecer no resultado final.

    Returns:
        Column: Expressão Spark com o resultado de ``concat_ws`` quando todas
        as colunas estiverem preenchidas, ou ``NULL`` caso qualquer uma delas
        seja nula.

    Notes:
        Regra de negócio: um identificador composto só é válido quando todos
        os seus componentes existem; a presença de um único componente nulo
        invalida o composto inteiro.
    """
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
    """Monta uma coluna composta a partir de múltiplos componentes de origem.

    Objetivo da operação:
        Padronizar a construção de identificadores compostos (ex.: célula ou
        IMSI/IMEI) usados por diferentes fornecedores, tratando componentes em
        branco como nulos e aplicando zero-padding em campos que exigem largura
        fixa antes da concatenação final via ``_concat_or_null``.

    Args:
        separator: Separador utilizado entre os componentes concatenados.
        components: Sequência de componentes do composto. Cada item pode ser
            o nome (``str``) de uma coluna do DataFrame ou uma expressão
            ``Column`` já calculada (ex.: um literal de MCC).
        padded_columns: Subconjunto de ``components`` (identificados por nome
            de coluna) que deve receber zero-padding à esquerda até 5
            caracteres antes da concatenação.

    Returns:
        Column: Expressão Spark com o composto final, ou ``NULL`` caso algum
        componente esteja ausente/em branco.

    Notes:
        Regra de negócio: o zero-padding de 5 caracteres reflete o tamanho
        máximo esperado para campos como LAC/CI/TAC em formato decimal,
        garantindo largura fixa e comparável entre fornecedores.
    """
    columns = []
    for component in components:
        column = _null_if_blank(component) if isinstance(component, str) else component
        if isinstance(component, str) and component in padded_columns:
            column = F.lpad(column, 5, "0")
        columns.append(column)

    return _concat_or_null(separator, *columns)


def _concat_date_time(
    df,
    start_date="_data",
    start_time="_hora",
    stop_date="_data_fim",
    stop_time="_hora_fim",
):
    """Combina colunas de data e hora em campos textuais intermediários.

    Args:
        df: DataFrame de entrada.
        start_date: Nome da coluna contendo a data de início.
        start_time: Nome da coluna contendo a hora de início.
        stop_date: Nome da coluna contendo a data de término.
        stop_time: Nome da coluna contendo a hora de término.


    Returns:
        DataFrame: Cópia do DataFrame de entrada com ``data_hora`` e
        ``data_hora_fim`` adicionadas ou atualizadas. A conversão dessas
        strings para timestamp é realizada posteriormente por
        ``_apply_standard_pipeline``.
    """
    return df.withColumns(
        {
            "data_hora": _build_composite_column(
                separator=" ",
                components=(start_date, start_time),
            ),
            "data_hora_fim": _build_composite_column(
                separator=" ",
                components=(stop_date, stop_time),
            ),
        }
    )


def _extract_cell_info(
    df,
    col_name,
    out_col_tec: str | None = "_tecnologia_celula",
    out_col_cell_id: str | None = "_id_celula_hex",
):
    """Extrai informações de célula a partir da coluna de rede.

    Args:
        df: DataFrame de entrada.
        col_name: Nome da coluna contendo a informação de rede.
        out_col_tec: Nome da coluna de saída para a tecnologia da célula, ou
            ``None`` para não gerar essa coluna.
        out_col_cell_id: Nome da coluna de saída para o id hexadecimal da
            célula, ou ``None`` para não gerar essa coluna.

    Returns:
        DataFrame: Cópia do DataFrame de entrada com as colunas
        as colunas especificadas em ``out_col_tec`` e ``out_col_cell_id`` adicionadas.

    Raises:
        ValueError: Se ``df`` ou ``col_name`` não forem informados, ou se
            ``col_name`` não existir entre as colunas de ``df``.
    """
    if df is None:
        raise ValueError("O parâmetro 'df' é obrigatório e não foi informado.")
    if not col_name:
        raise ValueError("O parâmetro 'col_name' é obrigatório e não foi informado.")
    if col_name not in df.columns:
        raise ValueError(
            f"A coluna de origem '{col_name}' não existe no DataFrame. "
            f"Colunas disponíveis: {df.columns}"
        )

    network_info = F.col(col_name)
    new_columns = {}
    if out_col_tec is not None:
        new_columns[out_col_tec] = F.split(network_info, ";").getItem(0)
    if out_col_cell_id is not None:
        new_columns[out_col_cell_id] = F.regexp_extract(
            network_info, _CELL_EXTRACT_PATTERN, 1
        )

    return df.withColumns(new_columns)


def _format_cell_id(df, col_name, out_col, gnb_id_bits=26, output_format="default"):
    """Formata identificadores de célula hexadecimais em MCC-MNC-área-célula.

    Objetivo da operação:
        Decodificar o identificador de célula bruto (em hexadecimal) conforme
        a tecnologia de acesso, reconhecida pelo comprimento da string de
        entrada, produzindo uma representação textual única e comparável
        entre 3G (UTRAN), 4G (ECGI) e 5G (NCGI).

    Args:
        df: DataFrame de origem contendo a coluna ``col_name`` a decodificar.
        col_name: Nome da coluna com o identificador de célula bruto em
            hexadecimal.
        out_col: Nome da coluna de saída onde o identificador formatado será
            gravado (pode coincidir com ``col_name`` para sobrescrever).
        gnb_id_bits: Quantidade de bits reservados ao identificador do gNB
            dentro do NCGI (5G). Os bits restantes (até completar 36) são
            atribuídos ao Cell ID. Valor padrão de 26 bits segue a convenção
            usual 3GPP para NCGI de 36 bits.
        output_format: Formato de saída desejado para o identificador de célula.
            Pode assumir valores como ``default`` (padrão) ou outros formatos suportados pelo sistema.
            default: Mantém o formatação padrão ``mcc-mnc-area-celula``.
            tim: Utiliza os formatos da prestadora TIM para identificadores 3G/4G/5G:
                3G: _not implemented_
                4G: mcc-mnc-eci
                5G: _not implemented_

    Returns:
        DataFrame: Cópia do DataFrame de entrada com a coluna ``out_col``
        adicionada/atualizada, contendo o identificador formatado no padrão
        ``mcc-mnc-area-celula``, o valor original (quando o comprimento não
        corresponde a nenhum layout conhecido) ou ``NULL`` quando a formatação
        resultar em string vazia.

    Notes:
        - Algoritmo não trivial: o comprimento da string (13, 16 ou 20
          caracteres) determina qual layout (3G/4G/5G) é aplicado; os campos
          binários do NCGI são extraídos via deslocamento e máscara de bits
          (``shiftright``/``bitwiseAND``) para separar gNB ID e Cell ID.
        - Anotação de manutenção: ``gnb_id_bits`` deve ser ajustado caso a
          operadora utilize uma partição de bits diferente da convenção
          padrão 26/10 para NCGI.
    """
    col = F.col(col_name)
    length = F.length(col)

    _check_output_format(output_format)

    # ---- 3G (UTRAN, 13 chars) ----
    tac_3g = F.conv(F.substring(col, 6, 4), 16, 10).cast("long")
    ci_3g = F.conv(F.substring(col, 10, 4), 16, 10).cast("long")
    ci_formatted = F.concat_ws(
        "-",
        F.substring(col, 1, 3),  # mcc
        F.substring(col, 4, 2),  # mnc
        F.lpad(tac_3g.cast("string"), 5, "0"),  # tac (16 bits)
        F.lpad(ci_3g.cast("string"), 5, "0"),  # ci (16 bits)
    )
    ci_formatted_tim = F.concat_ws(
        "-",
        F.substring(col, 1, 3),  # mcc
        F.substring(col, 4, 2),  # mnc
        tac_3g.cast("string"),  # tac (16 bits)
        ci_3g.cast("string"),  # ci (16 bits)
    )

    # ---- 4G (ECGI, 16 chars) ----
    ecgi_val = F.conv(F.substring(col, 10, 7), 16, 10).cast("long")
    ecgi_formatted = F.concat_ws(
        "-",
        F.substring(col, 1, 3),  # mcc
        F.substring(col, 4, 2),  # mnc
        F.lpad(
            (ecgi_val / 256).cast("long").cast("string"), 7, "0"
        ),  # enb_id (20 bits)
        F.lpad((ecgi_val % 256).cast("string"), 3, "0"),  # cell_id (8 bits)
    )
    ecgi_formatted_tim = F.concat_ws(
        "-",
        F.substring(col, 1, 3),  # mcc
        F.substring(col, 4, 2),  # mnc
        ecgi_val.cast("string"),
    )

    # ---- 5G (NCGI, 20 chars) ----
    # NCGI = 36 bits totais. gNB ID = 26 bits (default), Cell ID = 36 - 26 = 10 bits
    cell_id_bits = 36 - gnb_id_bits  # 10
    cell_id_mask = (1 << cell_id_bits) - 1  # 0x3FF = 1023

    ncgi_val = F.conv(F.substring(col, 12, 9), 16, 10).cast("long")
    ncgi_formatted = F.concat_ws(
        "-",
        F.substring(col, 1, 3),  # mcc
        F.substring(col, 4, 2),  # mnc
        F.lpad(
            F.shiftright(ncgi_val, cell_id_bits).cast("string"), 8, "0"
        ),  # gnb_id (26 bits)
        F.lpad(
            ncgi_val.bitwiseAND(cell_id_mask).cast("string"), 4, "0"
        ),  # cell_id (10 bits)
    )

    if output_format == "tim":
        formatted_cell_id = (
            F.when(length == 13, ci_formatted_tim)
            .when(length == 16, ecgi_formatted_tim)
            .when(length == 20, ncgi_formatted)
            .otherwise(col)
        )
    else:
        formatted_cell_id = (
            F.when(length == 13, ci_formatted)
            .when(length == 16, ecgi_formatted)
            .when(length == 20, ncgi_formatted)
            .otherwise(col)
        )

    return df.withColumn(
        out_col,
        F.when(formatted_cell_id == "", F.lit(None)).otherwise(formatted_cell_id),
    )


class CDRTransformer(CDRBaseTransformer):
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
    def transform_cdr_ericsson(
        self, source_file: str, target_file: str, output_format: str = "default"
    ) -> str:
        """Transforma CDR Ericsson para o contrato padronizado do domínio.

        Objetivo da operação:
            Converter a duração no formato ``HH:mm:ss`` para segundos inteiros
            e aplicar o pipeline padrão de normalização.

        Args:
            source_file: Caminho parquet com CDRs Ericsson de entrada.
            target_file: Caminho parquet de saída transformada.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Notes:
            - Regra de negócio: duração ausente resulta em ``0``.
            - Efeito colateral: grava o resultado em ``target_file``.
            - Anotação de manutenção: se o formato de duração mudar na origem,
              este cálculo deve ser revisado antes do pipeline comum.
        """

        _check_output_format(output_format)

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

        df = _concat_date_time(df, stop_date="_data")

        # Células da TIM não devem ter valores preenchidos com zeros à esquerda para lac e ci/sac.
        if output_format == "tim":
            padded_origin_cells = ()
            padded_destination_cells = ()
        else:
            padded_origin_cells = ("celula_origem_lac", "celula_origem_ci_sac")
            padded_destination_cells = ("celula_destino_lac", "celula_destino_ci_sac")

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
                    padded_origin_cells,
                ),
                "celula_destino": _build_composite_column(
                    "-",
                    (
                        "celula_destino_mcc",
                        "celula_destino_mnc",
                        "celula_destino_lac",
                        "celula_destino_ci_sac",
                    ),
                    padded_destination_cells,
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

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return target_file

    @log_operation
    def transform_cdr_lte_huawei_tim(self, source_file: str, target_file: str) -> str:
        """Transforma CDR TIM LTE Huawei para o contrato padronizado do domínio.

        Objetivo da operação:
            Extrair números e autenticação de campos JSON/SIP, remover prefixos
            dos números ATS e preparar metadados de rede antes da normalização
            central.

        Args:
            source_file: Caminho parquet com CDRs TIM LTE Huawei de entrada.
            target_file: Caminho parquet de saída transformada.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Notes:
            - Efeito colateral: grava o resultado em ``target_file``.
            - A regra de remoção de prefixo aplica ``substr(3, 9999)`` aos
              números ATS sem autenticação, removendo os dois primeiros
              caracteres da string.
            - Anotação de manutenção: os ramos de ATS e IBCF dependem da
              coluna ``tipo_cdr``. O contrato ``lte_huawei_tim`` da extração
              padrão disponibiliza ``_tipo_cdr``; este método não realiza essa
              renomeação.
            - Registros ``aTSRecord`` e ``iBCFRecord`` têm números e
              autenticação extraídos por regras distintas. Os atributos de célula, IMEI e
              IMSI são atribuídos à origem ou ao destino apenas para os papéis
              ``oRIGINATING-ROLE`` e ``tERMINATING-ROLE``.
            - Os timestamps são truncados aos 19 primeiros caracteres antes do pipeline
              comum. O IMSI é obtido apenas do primeiro objeto do JSON em ``_info_imsi``
              quando seu tipo é ``eND-USER-IMSI``.
            - ``codigo_resposta_sip`` é preenchido somente para valores de
              ``_status_chamada`` maiores ou iguais a 200; a mesma coluna é então
              classificada em faixas e códigos específicos para compor ``status_chamada``.
        """
        date_time_fmt = "yyyy-MM-dd HH:mm:ss"
        df = self.spark.read.parquet(source_file)

        is_ats = F.col("tipo_cdr") == "aTSRecord"
        is_ibcf = F.col("tipo_cdr") == "iBCFRecord"
        is_originating = F.col("tipo_chamada") == "oRIGINATING-ROLE"
        is_terminating = F.col("tipo_chamada") == "tERMINATING-ROLE"

        df = df.withColumns(
            {
                "_numero_origem_ats": F.when(
                    is_ats,
                    F.regexp_replace(
                        F.get_json_object(F.col("_numero_origem"), "$[0].tEL-URI"),
                        "(.)(.)",
                        "$2$1",
                    ),
                ),
                "_numero_origem_ibcf": F.when(
                    is_ibcf,
                    F.regexp_extract(
                        F.get_json_object(F.col("_numero_origem"), "$[0].sIP-URI"),
                        r"sip:([0-9]+)[@;]",
                        1,
                    ),
                ),
                # Data e hora nos CDR Tim Huawei trazem informação de fuso horário no final da string,
                # portanto, é necessário truncar os últimos caracteres para manter apenas a parte relevante.
                "data_hora": F.left(F.col("data_hora"), F.lit(19)),
                "data_hora_fim": F.left(F.col("data_hora_fim"), F.lit(19)),
            }
        )

        # Para ATS sem autenticação, ``substr(3, 9999)`` remove os dois
        # primeiros caracteres do número extraído antes da normalização comum.
        ats_calling_party = F.when(
            F.col("_numero_origem_ats_auth").isNotNull(),
            F.regexp_extract(F.col("_numero_origem_ats_auth"), r":\+?([0-9]+)", 1),
        ).otherwise(F.col("_numero_origem_ats").substr(3, 9999))
        raw_ats_calling_party = F.when(
            F.col("_numero_origem_ats_auth").isNotNull(),
            F.col("_numero_origem_ats_auth"),
        ).otherwise(F.col("_numero_origem_ats"))
        ibcf_calling_party = F.regexp_extract(
            F.col("_numero_origem_ibcf"), r"sip:\+?([0-9]+)", 1
        )

        df = df.withColumns(
            {
                "numero_origem": F.when(is_ats, ats_calling_party).otherwise(
                    ibcf_calling_party
                ),
                "_numero_origem_original": F.when(
                    is_ats, raw_ats_calling_party
                ).otherwise(F.col("_numero_origem_ibcf")),
                "numero_destino": F.when(
                    is_ats,
                    F.col("_numero_destino_ats").substr(3, 9999),
                ).otherwise(
                    F.regexp_extract(
                        F.col("_numero_destino_ibcf"), r"sip:\+?([0-9]+)", 1
                    )
                ),
                "_numero_destino_original": F.when(
                    is_ats, F.col("_numero_destino_ats")
                ).otherwise(F.col("_numero_destino_ibcf")),
                "_autenticacao": F.when(
                    is_ats,
                    F.regexp_extract(
                        F.col("_numero_origem_ats_auth"),
                        _AUTH_EXTRACT_PATTERN,
                        0,
                    ),
                ).otherwise(
                    F.regexp_extract(F.col("_numero_origem"), _AUTH_EXTRACT_PATTERN, 0)
                ),
            }
        )

        df = _extract_cell_info(
            df,
            "_informacao_rede",
            out_col_tec="_tecnologia_celula",
            out_col_cell_id="_id_celula_hex",
        )
        df = _format_cell_id(df, "_id_celula_hex", "_id_celula", output_format="tim")

        df = df.withColumn(
            "_imei",
            F.when(F.col("_info_imei") == "iMEI", F.col("_imei")).otherwise(
                F.lit(None)
            ),
        )

        schema_imsi = T.ArrayType(
            T.StructType(
                [
                    T.StructField("info_type", T.StringType()),
                    T.StructField("info_value", T.StringType()),
                ]
            )
        )

        df = (
            df.withColumn(
                "_imsi_parsed",
                F.from_json(F.col("_info_imsi"), schema_imsi).getItem(0),
            )
            .withColumn(
                "_imsi",
                F.when(
                    F.col("_imsi_parsed.info_type") == "eND-USER-IMSI",
                    F.col("_imsi_parsed.info_value"),
                ),
            )
            .drop("_imsi_parsed")
        )

        df = df.withColumns(
            {
                "celula_origem_hex": F.when(is_originating, F.col("_id_celula_hex")),
                "celula_destino_hex": F.when(is_terminating, F.col("_id_celula_hex")),
                "celula_origem": F.when(is_originating, F.col("_id_celula")),
                "celula_destino": F.when(is_terminating, F.col("_id_celula")),
                "tecnologia_celula_origem": F.when(
                    is_originating, F.col("_tecnologia_celula")
                ),
                "tecnologia_celula_destino": F.when(
                    is_terminating, F.col("_tecnologia_celula")
                ),
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

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return target_file

    @log_operation
    def transform_cdr_lte_ericsson_vivo(
        self, source_file: str, target_file: str
    ) -> str:
        """Transforma CDR Vivo LTE Ericsson para o contrato padronizado do domínio.

        Objetivo da operação:
            Executar o pré-processamento específico da Vivo LTE Ericsson para separar
            metadados embutidos e, em seguida, aplicar a normalização padrão.

        Args:
            source_file: Caminho parquet com CDRs Vivo LTE Ericsson de entrada.
            target_file: Caminho parquet de saída transformada.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Notes:
            - A coluna ``_numero_origem_original`` é dividida em ``;``: o
              primeiro trecho substitui ``numero_origem`` e o segundo é usado
              como ``_autenticacao``.
            - Os códigos de ``_tipo_chamada`` e ``_status_chamada`` conhecidos
              são convertidos para rótulos textuais. Hífens de IMEIs são
              removidos antes do pipeline comum.
            - ``_format_cell_id`` converte separadamente os identificadores
              hexadecimais de origem e destino para suas colunas de célula.
            - Efeito colateral: grava o resultado em ``target_file``.
        """
        date_time_fmt = "yyyyMMdd HHmmss"
        df = self.spark.read.parquet(source_file)

        df = _concat_date_time(df, stop_date="_data")

        # Tecnologia das células de origem e destino.
        # ID das células já existe no DataFrame original, portanto não precisamos extraí-lo novamente.
        df = _extract_cell_info(
            df,
            "_informacao_rede_origem",
            out_col_tec="tecnologia_celula_origem",
            out_col_cell_id=None,
        )
        df = _extract_cell_info(
            df,
            "_informacao_rede_destino",
            out_col_tec="tecnologia_celula_destino",
            out_col_cell_id=None,
        )

        # Extrair autenticação e prefixos adicionais dos números.
        # A autenticação está contida na coluna _numero_origem,
        # por exemplo: 551136128860;verstat=TN-Validation-Passe
        df = (
            df.withColumn("_split", F.split(F.col("_numero_origem_original"), ";"))
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

        df = _format_cell_id(df, "celula_origem_hex", "celula_origem")
        df = _format_cell_id(df, "celula_destino_hex", "celula_destino")

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return target_file

    @log_operation
    def transform_cdr_nokia(self, source_file: str, target_file: str) -> str:
        """Transforma CDR Nokia para o contrato padronizado do domínio.

        Objetivo da operação:
            Consolidar campos variantes de duração/data e ajustar números para
            cenários de encaminhamento antes do pipeline padrão.

        Args:
            source_file: Caminho parquet com CDRs Nokia de entrada.
            target_file: Caminho parquet de saída transformada.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Notes:
            - Regra de negócio: múltiplos campos ``_duracao*`` são reduzidos a
              uma única duração por registro via ``coalesce``; valores
              ``"FFFFFF"`` são tratados como nulos antes da redução.
            - Regra de negócio: para chamadas ``FORW``, o destino é derivado do
              campo ``numero_origem_encaminhamento``.
            - ``data_hora`` usa ``data_hora_alocacao_canal`` quando disponível
              e recorre a ``data_hora_referencia``. Para chamadas ``UCA``,
              ``data_hora_fim`` pode usar ``data_hora_desconexao`` quando essa
              coluna está presente no DataFrame.
            - Como o layout não fornece MCC/MNC, as células usam ``DEFAULT_MCC``
              e MNC conforme ``prestadora``: Claro recebe ``CLARO_MNC`` e Algar
              recebe ``ALGAR_MNC``. Outros valores de prestadora resultam em MNC
              nulo e, consequentemente, em célula composta nula.
            - ``_status_chamada`` é agrupado em faixas de códigos hexadecimais
              antes da aplicação do pipeline comum.
            - Efeito colateral: grava o resultado em ``target_file``.
            - Anotação de manutenção: divergências residuais com parser legado
              devem ser monitoradas em homologações futuras.
        """
        date_time_fmt = "dd/MM/yyyy HH:mm:ss"
        df = self.spark.read.parquet(source_file)

        # Cada tipo de CDR pode preencher uma coluna de duração distinta; a
        # primeira coluna não nula, na ordem do DataFrame, torna-se ``duracao``.
        duration_columns = [col for col in df.columns if col.startswith("_duracao")]

        # O sentinela ``FFFFFF`` é removido antes do ``coalesce`` para não ser
        # escolhido como duração válida.
        cleaned_duration_cols = [
            F.when(F.col(c) == "FFFFFF", F.lit(None)).otherwise(F.col(c))
            for c in duration_columns
        ]
        df = df.withColumn("duracao", F.coalesce(*cleaned_duration_cols))

        # Prioriza a alocação de canal e usa a referência como alternativa;
        # valores ausentes são normalizados pelo pipeline padrão.
        df = df.withColumn(
            "data_hora",
            F.coalesce(
                F.col("data_hora_alocacao_canal"),
                F.col("data_hora_referencia"),
            ),
        )
        # Em UCA, a desconexão substitui o fim da chamada somente quando a
        # coluna estiver disponível; o pipeline padrão trata valores inválidos.
        if "data_hora_desconexao" in df.columns:
            df = df.withColumn(
                "data_hora_fim",
                F.when(
                    F.col("tipo_chamada") == "UCA",
                    F.coalesce(
                        F.col("data_hora_desconexao"),
                        F.col("data_hora_fim"),
                    ),
                ).otherwise(F.col("data_hora_fim")),
            )

        # O número original supre a origem ausente. Em chamadas ``FORW``, o
        # destino é substituído pelo número de origem do encaminhamento.
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

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return target_file

    @log_operation
    def transform_cdr_ngn_huawei(self, source_file: str, target_file: str) -> str:
        """Transforma registros do layout NGN Huawei usando o pipeline padrão.

        Combina os campos de data e hora extraídos, converte os códigos de tipo
        e status de chamada conhecidos para rótulos textuais e delega a
        normalização restante ao pipeline comum.

        Args:
            source_file: Caminho do arquivo de entrada no formato NGN Huawei.
            target_file: Diretório de saída em parquet padronizado.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Notes:
            - ``data_hora`` resulta da combinação de ``_data`` com ``_hora``;
              ``data_hora_fim`` combina ``_data_fim`` com a mesma coluna
              ``_hora``.
            - Códigos não previstos em ``_tipo_chamada`` e ``_status_chamada``
              recebem o rótulo ``"unknown"``.
            - Efeito colateral: grava o resultado em ``target_file``.
        """

        date_time_fmt = "ddMMyyyy HHmmss"
        df = self.spark.read.parquet(source_file)

        df = _concat_date_time(df)

        df = df.withColumns(
            {
                "tipo_chamada": F.when(
                    F.col("_tipo_chamada") == "01", F.lit("intra_office")
                )
                .when(F.col("_tipo_chamada") == "02", F.lit("incoming_office"))
                .when(F.col("_tipo_chamada") == "03", F.lit("outgoing_office"))
                .when(F.col("_tipo_chamada") == "04", F.lit("tandem"))
                .when(F.col("_tipo_chamada") == "05", F.lit("new_service"))
                .otherwise(F.lit("unknown")),
                "status_chamada": F.when(
                    F.col("_status_chamada") == "00", F.lit("caller party on-hook")
                )
                .when(F.col("_status_chamada") == "01", F.lit("called party on-hook"))
                .when(F.col("_status_chamada") == "02", F.lit("abnormal"))
                .otherwise(F.lit("unknown")),
            }
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        self._write_parquet(df, target_file)

        return target_file
