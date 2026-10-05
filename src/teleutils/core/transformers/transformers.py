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
    - Pré-processamento de layouts SMP Ericsson, Huawei VoLTE e Nokia GSM.
    - Pré-processamento de layouts STFC Huawei NGN, Vivo FCDR e Oi/Claro.
    - Composição de datas, células, IMSIs e IMEIs e classificação de códigos.

Dependências relevantes:
    - pyspark.sql (SparkSession e funções colunares)
    - teleutils._logging.log_operation
    - teleutils.core.transformers.base_transformer.CDRBaseTransformer

Notes:
    Os métodos de layout leem Parquet intermediário, aplicam ajustes específicos
    e delegam a normalização comum ao transformador base. Esse pipeline completa
    colunas ausentes, converte datas segundo a máscara do layout, aplica
    ``MIN_SAFE_DATE``, normaliza duração e números, deriva autenticação e preenche
    nulos da chave primária. A normalização telefônica usa a UDF
    ``spark_normalize_number`` e produz números formatados e indicadores de
    validade, preservando os números originais quando disponibilizados.

    As expressões constroem planos distribuídos; a escrita materializa os dados.
    ``_write_parquet`` seleciona, converte e renomeia as colunas de
    ``TARGET_SCHEMA``, descarta campos fora do contrato e sobrescreve o destino
    particionado por ``no_tipo_chamada``. Os métodos retornam o caminho de saída,
    não um DataFrame. ``log_operation`` registra início, sucesso e falhas,
    relançando exceções.

    ``transform`` procura funções no namespace global do módulo, não métodos da
    classe. Este arquivo não define funções globais ``transform_<cdr_schema>``;
    os métodos de layout podem ser chamados diretamente.

Example:
    >>> transformer = CDRTransformer(spark)
    >>> destino = transformer.transform_smp_gsm_nokia("/tmp/in", "/tmp/out")
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
_VALID_OUTPUT_FORMATS = {
    "default",
    "smp_ericsson_gsm_tim",
    "smp_huawei_volte_tim",
    "smp_nokia_algar",
}


def _check_output_format(
    output_format: str, valid_output_formats: set[str] = _VALID_OUTPUT_FORMATS
):
    """Verifica se o formato de saída fornecido é válido.

    Args:
        output_format: Formato de saída a ser verificado.
        valid_output_formats: Conjunto de formatos aceitos. O padrão contém
            ``default``, ``smp_ericsson_gsm_tim``, ``smp_huawei_volte_tim`` e
            ``smp_nokia_algar``; a comparação é exata.

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
        as colunas forem não nulas, ou ``NULL`` caso qualquer uma delas
        seja nula.

    Raises:
        TypeError: Se nenhum componente for fornecido, pois ``reduce`` não
            recebe um valor inicial para uma sequência vazia.

    Notes:
        Regra de negócio: um identificador composto só é válido quando todos
        os seus componentes existem; a presença de um único componente nulo
        invalida o composto inteiro. Strings vazias não são tratadas aqui;
        ``_build_composite_column`` normaliza os componentes recebidos por nome.
    """
    cols = [F.col(c) if isinstance(c, str) else c for c in columns]

    has_any_null = reduce(or_, [c.isNull() for c in cols])

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
        componente seja nulo ou um componente recebido por nome esteja em branco.

    Raises:
        TypeError: Se ``components`` estiver vazio, pela redução sem valor
            inicial em ``_concat_or_null``.

    Notes:
        Componentes recebidos como expressões não passam por ``_null_if_blank``
        nem pelo preenchimento. Nos componentes nomeados, valores não vazios
        preservam o conteúdo original, sem retirar espaços das bordas.
        ``lpad`` produz largura de 5 caracteres e também trunca valores maiores;
        não há validação prévia dessa largura.
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
        stop_date: Nome da coluna contendo a data de término. Padrão: ``_data_fim``.
        stop_time: Nome da coluna contendo a hora de término. Padrão: ``_hora_fim``.

    Returns:
        DataFrame: Cópia do DataFrame de entrada com ``data_hora`` adicionada
        ou atualizada. ``data_hora_fim`` também é adicionada ou atualizada quando
        as colunas indicadas por ``stop_date`` e ``stop_time`` existem. A conversão dessas
        strings para timestamp é realizada posteriormente por
        ``_apply_standard_pipeline``.

    Notes:
        Os componentes são unidos por espaço e o resultado é nulo quando algum
        deles é nulo ou está em branco. As colunas de início são necessárias;
        se faltar uma coluna de término, ``data_hora_fim`` permanece inalterada
        ou ausente, para tratamento posterior pelo pipeline comum.
    """
    columns = {
        "data_hora": _build_composite_column(
            separator=" ",
            components=(start_date, start_time),
        )
    }

    if stop_date in df.columns and stop_time in df.columns:
        columns["data_hora_fim"] = _build_composite_column(
            separator=" ",
            components=(stop_date, stop_time),
        )

    return df.withColumns(columns)


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
        especificadas em ``out_col_tec`` e ``out_col_cell_id`` adicionadas ou
        substituídas. A tecnologia é o primeiro trecho separado por ``;``;
        o identificador é o grupo hexadecimal encontrado após ``3gpp=``.

    Raises:
        ValueError: Se ``df`` ou ``col_name`` não forem informados, ou se
            ``col_name`` não existir entre as colunas de ``df``.

    Notes:
        ``regexp_extract`` retorna string vazia quando o padrão não é encontrado
        em uma entrada não nula. Esta função não converte esse resultado em nulo.
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
            atribuídos ao Cell ID. O padrão da implementação é 26 bits.
        output_format: Uma das chaves de ``_VALID_OUTPUT_FORMATS``. Para 3G,
            ``smp_ericsson_gsm_tim`` e ``smp_nokia_algar`` não completam os
            componentes de área e célula com zeros. Para 4G,
            ``smp_huawei_volte_tim`` produz ``mcc-mnc-eci``. As demais combinações
            usam a representação detalhada; 5G independe dessa opção.

    Returns:
        DataFrame: Cópia do DataFrame de entrada com a coluna ``out_col``
                adicionada/atualizada, contendo o identificador formatado segundo
                ``output_format``, o valor original (quando o comprimento não
        corresponde a nenhum layout conhecido) ou ``NULL`` quando a formatação
        resultar em string vazia.

    Raises:
        ValueError: Se ``output_format`` não estiver em ``_VALID_OUTPUT_FORMATS``.
            Uma partição com ``gnb_id_bits > 36`` também produz deslocamento
            negativo ao construir a máscara em Python.

    Notes:
        O comprimento, não a coluna de tecnologia, escolhe o layout. Nos três
        casos, MCC ocupa os caracteres 1 a 3 e MNC os caracteres 4 e 5.
        Em 3G (13 caracteres), área e célula são convertidas do hexadecimal
        nas posições 6 e 10, com largura 4; o formato detalhado usa largura 5.
        Em 4G (16 caracteres), o ECI vem da posição 10, com largura 7: a divisão
        inteira por 256 produz o eNB e o resto produz a célula, preenchidos com
        zeros até larguras 7 e 3. Em 5G (20 caracteres), os 9 caracteres a partir
        da posição 12 formam o NCGI; deslocamento e máscara separam gNB e célula,
        preenchidos até larguras 8 e 4.

        ``gnb_id_bits`` não tem validação de faixa e a máscara é construída
        independentemente da tecnologia dos registros. As composições usam
        ``concat_ws``, que ignora componentes nulos: não há exigência de todos
        os componentes presentes, diferentemente de ``_concat_or_null``.
    """
    col = F.col(col_name)
    length = F.length(col)

    _check_output_format(output_format)

    # ---- 3G (UTRAN, 13 chars) ----
    tac_3g = F.conv(F.substring(col, 6, 4), 16, 10).cast("long")
    ci_3g = F.conv(F.substring(col, 10, 4), 16, 10).cast("long")

    if output_format in ("smp_ericsson_gsm_tim", "smp_nokia_algar"):
        ci_formatted = F.concat_ws(
            "-",
            F.substring(col, 1, 3),  # mcc
            F.substring(col, 4, 2),  # mnc
            tac_3g.cast("string"),  # tac (16 bits)
            ci_3g.cast("string"),  # ci (16 bits)
        )
    else:
        ci_formatted = F.concat_ws(
            "-",
            F.substring(col, 1, 3),  # mcc
            F.substring(col, 4, 2),  # mnc
            F.lpad(tac_3g.cast("string"), 5, "0"),  # tac (16 bits)
            F.lpad(ci_3g.cast("string"), 5, "0"),  # ci (16 bits)
        )

    # ---- 4G (ECGI, 16 chars) ----
    ecgi_val = F.conv(F.substring(col, 10, 7), 16, 10).cast("long")

    if output_format == "smp_huawei_volte_tim":
        ecgi_formatted = F.concat_ws(
            "-",
            F.substring(col, 1, 3),  # mcc
            F.substring(col, 4, 2),  # mnc
            ecgi_val.cast("string"),
        )
    else:
        ecgi_formatted = F.concat_ws(
            "-",
            F.substring(col, 1, 3),  # mcc
            F.substring(col, 4, 2),  # mnc
            F.lpad(
                (ecgi_val / 256).cast("long").cast("string"), 7, "0"
            ),  # enb_id (20 bits)
            F.lpad((ecgi_val % 256).cast("string"), 3, "0"),  # cell_id (8 bits)
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
    """Transformador de CDRs intermediários com regras por fornecedor/layout.

    A classe especializa o transformador base para lidar com peculiaridades de
    layouts de entrada em Parquet. Cada método de
    transformação encapsula ajustes de campos que antecedem a execução do
    pipeline padrão.

    Contexto de uso:
        - Etapa de transformação após extração/parsing bruto dos CDRs.
        - Invocada por rotinas de ingestão para geração do dataset curado.

    Attributes:
        spark:
            Nova sessão criada por ``spark.newSession()`` no construtor base,
            configurada com ``spark.sql.timestampType=TIMESTAMP_NTZ``.
        default_mcc: Expressão literal de ``DEFAULT_MCC`` para células Nokia.
        algar_mnc: Expressão literal de ``ALGAR_MNC`` para células Nokia/Algar.
        claro_mnc: Expressão literal de ``CLARO_MNC`` para células Nokia/Claro.
        min_safe_date: Expressão de ``MIN_SAFE_DATE`` herdada para datas e chave.
        null_sentinel_value: Expressão do sentinela textual herdada para a chave.

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
            spark: Sessão Spark a partir da qual o construtor base cria a sessão
                usada pela instância.

        Notes:
            Configura a nova sessão para timestamps sem fuso e cria expressões
            literais de MCC/MNC. Não lê nem grava arquivos na inicialização.
        """

        super().__init__(spark)
        self.default_mcc = F.lit(DEFAULT_MCC)
        self.algar_mnc = F.lit(ALGAR_MNC)
        self.claro_mnc = F.lit(CLARO_MNC)

    @log_operation
    def transform(
        self,
        source_file: str,
        target_file: str,
        cdr_schema: str,
        output_format: str = "default",
    ) -> str:
        """Valida o formato e encaminha a chamada a um transformador global.

        Args:
            source_file: Caminho parquet com CDRs de entrada.
            target_file: Caminho parquet de saída transformada.
            cdr_schema: Sufixo usado na busca por ``transform_<cdr_schema>``
                no namespace global deste módulo.
            output_format: Chave aceita por ``_check_output_format``, repassada
                ao transformador encontrado. O padrão é ``default``.

        Returns:
            str: Retorno do transformador global encontrado, esperado como o
                caminho de saída; este método não grava diretamente.

        Raises:
            ValueError: Se o formato não for aceito ou não houver um elemento
                global com o nome solicitado.

        Notes:
            A busca não consulta os métodos da instância. Este arquivo define
            os transformadores de layout como métodos de ``CDRTransformer``,
            não como funções globais, portanto eles não são encontrados por
            esta implementação. A existência do elemento global é verificada,
            mas sua capacidade de chamada não é validada.
        """

        _check_output_format(output_format)

        transformer = globals().get(f"transform_{cdr_schema}")
        if transformer is None:
            raise ValueError(
                f"Transformador para o schema '{cdr_schema}' não encontrado."
            )
        return transformer(self, source_file, target_file, output_format=output_format)

    @log_operation
    def transform_smp_ericsson_gsm(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Transforma CDR SMP GSM Ericsson para o contrato padronizado do domínio.

        Objetivo da operação:
            Converter a duração no formato ``HH:mm:ss`` para segundos inteiros
            e aplicar o pipeline padrão de normalização.

        Args:
            source_file: Caminho parquet com CDRs Ericsson de entrada.
            target_file: Caminho parquet de saída transformada.
            **kwargs: ``output_format`` é consultado com padrão ``"csv"``;
                apenas o valor ``"tim"`` desativa o preenchimento de LAC e
                CI/SAC com zeros. As demais opções são ignoradas.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Notes:
            - Regra de negócio: duração ausente resulta em ``0``.
            - A duração usa substrings nas posições 1, 4 e 7, com largura 2;
                o método não valida o padrão ``HH:mm:ss`` antes das conversões.
            - Datas são combinadas com horas e interpretadas por
                ``yy-MM-dd HH:mm:ss``. O término usa ``_data`` e ``_hora_fim``.
            - Células de origem/destino concatenam MCC, MNC, LAC e CI/SAC com
                hífens. IMSIs concatenam MCC, MNC e MSIN, e IMEIs concatenam TAC
                e SN, sem separador. Componentes nomeados em branco viram nulos;
                qualquer componente nulo torna o identificador composto nulo.
            - Grava Parquet com sobrescrita pelo pipeline de escrita herdado.
                ``output_format`` não altera o formato de arquivo nem é validado
                neste método; ``"csv"`` e ``"tim"`` não pertencem ao conjunto
                aceito pelo despachante ``transform``.
        """

        output_format = kwargs.get("output_format", "csv")

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

        # Apenas a opção literal "tim" desativa o preenchimento de LAC e CI/SAC.
        padded_origin_cells: tuple[str, ...]
        padded_destination_cells: tuple[str, ...]

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
    def transform_smp_gsm_nokia(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Transforma CDR SMP GSM Nokia para o contrato padronizado do domínio.

        Objetivo da operação:
            Consolidar campos variantes de duração/data e ajustar números para
            cenários de encaminhamento antes do pipeline padrão.

        Args:
            source_file: Caminho parquet com CDRs Nokia de entrada.
            target_file: Caminho parquet de saída transformada.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Notes:
            - Regra de negócio: múltiplos campos ``_duracao*`` são reduzidos a
                uma única duração por registro via ``coalesce``, na ordem das
                colunas do DataFrame; valores ``"FFFFFF"`` são tratados como nulos
                antes da redução.
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
            - LAC e CI são preenchidos até largura 5. As datas usam a máscara
                ``dd/MM/yyyy HH:mm:ss`` no pipeline comum.
            - ``_resultado_chamada`` é comparado com limites numéricos definidos
                em hexadecimal: 0x0000 a 0x03FF vira ``normal clearing``;
                0x0400 a 0x07FF, ``internal congestion``; 0x0800 a 0x0BFF,
                ``external congestion``; 0x0C00 a 0x0FFF, ``subscriber errors``;
                a partir de 0x1000, ``event codes``. Demais casos resultam em nulo.
                Não há conversão explícita de uma string hexadecimal nessa etapa.
            - O pré-processamento exige as colunas referenciadas e não verifica
                se há pelo menos uma coluna ``_duracao*`` antes de ``coalesce``.
            - Grava Parquet com sobrescrita pelo pipeline de escrita herdado.
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
        nokia_mnc = F.when(F.col("prestadora") == "claro", self.claro_mnc).when(
            F.col("prestadora") == "algar", self.algar_mnc
        )
        df = (
            df.withColumn("_nokia_mnc", nokia_mnc)
            .withColumns(
                {
                    "celula_origem": _build_composite_column(
                        "-",
                        (
                            self.default_mcc,
                            F.col("_nokia_mnc"),
                            "celula_origem_lac",
                            "celula_origem_ci",
                        ),
                        ("celula_origem_lac", "celula_origem_ci"),
                    ),
                    "celula_destino": _build_composite_column(
                        "-",
                        (
                            self.default_mcc,
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

        # Os limites hexadecimais são constantes Python; a coluna não é decodificada aqui.
        # +--------------------+---------------------+
        # | _resultado_chamada | descrição           |
        # +--------------------+---------------------+
        # | 0000H - 03FFH      | normal clearing     |
        # | 0400H - 07FFH      | internal congestion |
        # | 0800H - 0BFFH      | external congestion |
        # | 0C00H - 0FFFH      | subscriber errors   |
        # | 1000H -            | event codes         |
        # +--------------------+---------------------+
        df = df.withColumn(
            "resultado_chamada",
            F.when(
                (F.col("_resultado_chamada") >= F.lit(int("0000", 16)))
                & (F.col("_resultado_chamada") <= F.lit(int("03FF", 16))),
                F.lit("normal clearing"),
            )
            .when(
                (F.col("_resultado_chamada") >= F.lit(int("0400", 16)))
                & (F.col("_resultado_chamada") <= F.lit(int("07FF", 16))),
                F.lit("internal congestion"),
            )
            .when(
                (F.col("_resultado_chamada") >= F.lit(int("0800", 16)))
                & (F.col("_resultado_chamada") <= F.lit(int("0BFF", 16))),
                F.lit("external congestion"),
            )
            .when(
                (F.col("_resultado_chamada") >= F.lit(int("0C00", 16)))
                & (F.col("_resultado_chamada") <= F.lit(int("0FFF", 16))),
                F.lit("subscriber errors"),
            )
            .when(
                F.col("_resultado_chamada") >= F.lit(int("1000", 16)),
                F.lit("event codes"),
            )
            .otherwise(F.lit(None)),
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return target_file

    @log_operation
    def transform_smp_huawei_volte_tim(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Transforma CDR SMP Huawei VoLTE TIM para o contrato padronizado do domínio.

        Objetivo da operação:
            Extrair números e autenticação de campos JSON/SIP, remover prefixos
            dos números ATS e preparar metadados de rede antes da normalização
            central.

        Args:
            source_file: Caminho parquet com CDRs SMP Huawei VoLTE TIM de entrada.
            target_file: Caminho parquet de saída transformada.
            **kwargs: Opções adicionais aceitas, mas não utilizadas. O formato
                de célula é fixado em ``smp_huawei_volte_tim`` internamente.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Raises:
            ValueError: Se a coluna ``_informacao_rede`` não existir ao executar
                ``_extract_cell_info``.

        Notes:
            - Grava Parquet com sobrescrita pelo pipeline de escrita herdado.
            - A regra de remoção de prefixo aplica ``substr(3, 9999)`` aos
                números ATS, removendo os dois primeiros caracteres da string
                e invertendo em seguida os pares de caracteres por regex.
                Essa regra independe do conteúdo da autenticação. Para IBCF,
                extrai dígitos entre ``sip:`` e ``@`` ou ``;``; sem correspondência,
                ``regexp_extract`` retorna string vazia em entradas não nulas.
            - Os ramos de ATS e IBCF dependem da coluna ``tipo_cdr`` e dos valores
                literais ``aTSRecord`` e ``iBCFRecord``; este método não renomeia
                ``_tipo_cdr`` nem cria uma alternativa para essa coluna ausente.
            - Registros ``aTSRecord`` e ``iBCFRecord`` têm números e
              autenticação extraídos por regras distintas. Os atributos de célula, IMEI e
              IMSI são atribuídos à origem ou ao destino apenas para os papéis
              ``oRIGINATING-ROLE`` e ``tERMINATING-ROLE``.
            - Os timestamps são truncados aos 19 primeiros caracteres antes do pipeline
                comum, que usa ``yyyy-MM-dd HH:mm:ss``; a informação posterior,
                inclusive o fuso, não participa do parsing. O IMSI é obtido apenas do primeiro objeto do JSON em ``_info_imsi``
                quando seu tipo é ``eND-USER-IMSI``.
            - A origem ATS usa ``$[0].tEL-URI`` e a origem IBCF usa
                ``$[0].sIP-URI`` de ``_numero_origem_ats_ibcf``. Os valores anteriores
                à normalização são guardados nas colunas ``_numero_*_original``.
                A autenticação extrai ``verstat=`` seguido de letras ou hífens,
                de ``_numero_origem_auth`` para ATS e do campo compartilhado para
                os demais registros. IMEI só é mantido quando ``_info_imei == "iMEI"``.
            - ``codigo_resposta_sip`` é preenchido somente para valores de
              ``_resultado_chamada`` maiores ou iguais a 200; a mesma coluna é então
              classificada em faixas e códigos específicos para compor ``resultado_chamada``.
              Em IBCF, o código vem de ``SIP;cause=<dígitos>;``. A classificação
              cobre os intervalos (-400, -300] e (-300, -200], os códigos -3 a 5,
              200 e as faixas SIP de 201 a 699; demais valores resultam em nulo.
        """
        date_time_fmt = "yyyy-MM-dd HH:mm:ss"
        df = self.spark.read.parquet(source_file)

        df = df.withColumns(
            # Data e hora nos CDR Tim Huawei trazem informação de fuso horário no final da string,
            # portanto, é necessário truncar os últimos caracteres para manter apenas a parte relevante.
            {
                "data_hora": F.left(F.col("data_hora"), F.lit(19)),
                "data_hora_fim": F.left(F.col("data_hora_fim"), F.lit(19)),
            }
        )

        is_ats = F.col("tipo_cdr") == "aTSRecord"
        is_ibcf = F.col("tipo_cdr") == "iBCFRecord"

        is_originating = F.col("tipo_chamada") == "oRIGINATING-ROLE"
        is_terminating = F.col("tipo_chamada") == "tERMINATING-ROLE"

        # Regex para extrair o número de telefone do URI SIP.
        sip_extract_pattern = r"sip:([0-9]+)[@;]"

        # Números de origem ATS estão com os dígitos invertidos 2 a 2.
        # Por exemplo, o número "11551899781480F2" seria invertido para "115581998741082F".
        # 11-55-18-99-78-14-80-F2
        #  ↓  ↓  ↓  ↓  ↓  ↓  ↓  ↓
        # 11-55-81-99-87-41-08-2F
        # Além disso trazem prefixo 11 ou 14 que podem ser confundidos como os respectivos CN.
        # ``substr(3, 9999)`` remove os prefixos antes da normalização para evitar confundi-los com CN.
        extract_ats_calling_party = F.get_json_object(
            F.col("_numero_origem_ats_ibcf"), "$[0].tEL-URI"
        )
        ats_calling_party = F.regexp_replace(
            extract_ats_calling_party.substr(3, 9999),
            "(.)(.)",
            "$2$1",
        )
        ibfc_calling_party = F.regexp_extract(
            F.get_json_object(F.col("_numero_origem_ats_ibcf"), "$[0].sIP-URI"),
            sip_extract_pattern,
            1,
        )
        df = df.withColumn(
            "numero_origem",
            F.when(is_ats, ats_calling_party).when(is_ibcf, ibfc_calling_party),
        ).withColumn(
            "_numero_origem_original",
            F.when(is_ats, extract_ats_calling_party).otherwise(F.col("numero_origem")),
        )

        ats_called_party = F.regexp_replace(
            F.col("_numero_destino_ats").substr(3, 9999),
            "(.)(.)",
            "$2$1",
        )
        ibfc_called_party = F.regexp_extract(
            F.col("_numero_destino_ibcf"),
            sip_extract_pattern,
            1,
        )
        df = df.withColumn(
            "numero_destino",
            F.when(is_ats, ats_called_party).when(is_ibcf, ibfc_called_party),
        ).withColumn(
            "_numero_destino_original",
            F.when(is_ats, F.col("_numero_destino_ats")).otherwise(
                F.col("numero_destino")
            ),
        )

        df = df.withColumn(
            "_autenticacao",
            F.when(
                is_ats,
                F.regexp_extract(
                    F.col("_numero_origem_auth"),
                    _AUTH_EXTRACT_PATTERN,
                    0,
                ),
            ).otherwise(
                F.regexp_extract(
                    F.col("_numero_origem_ats_ibcf"), _AUTH_EXTRACT_PATTERN, 0
                )
            ),
        )

        df = _extract_cell_info(
            df,
            "_informacao_rede",
            out_col_tec="_tecnologia_celula",
            out_col_cell_id="_id_celula_hex",
        )
        df = _format_cell_id(
            df, "_id_celula_hex", "_id_celula", output_format="smp_huawei_volte_tim"
        )

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
            "_resultado_chamada",
            F.when(
                is_ibcf,
                F.regexp_extract(
                    F.col("_resultado_chamada"), r"SIP;cause=([0-9]+);", 1
                ).cast(T.IntegerType()),
            ).otherwise(F.col("_resultado_chamada").cast(T.IntegerType())),
        ).withColumn(
            "codigo_resposta_sip",
            F.when(
                F.col("_resultado_chamada")
                >= 200,  # códigos de resposta SIP válidos são >= 200
                F.col("_resultado_chamada"),
            ).otherwise(F.lit(None).cast(T.IntegerType())),
        )

        df = df.withColumn(
            "resultado_chamada",
            F.when(
                (F.col("_resultado_chamada") <= -300)
                & (F.col("_resultado_chamada") > -400),
                F.lit("Redirection"),
            )
            .when(
                (F.col("_resultado_chamada") <= -200)
                & (F.col("_resultado_chamada") > -300),
                F.lit("Final Response"),
            )
            .when(F.col("_resultado_chamada") == -3, F.lit("End of REGISTER dialog"))
            .when(F.col("_resultado_chamada") == -2, F.lit("End of SUBSCRIBE dialog"))
            .when(F.col("_resultado_chamada") == -1, F.lit("Successful transaction"))
            .when(F.col("_resultado_chamada") == 0, F.lit("Normal end of session"))
            .when(F.col("_resultado_chamada") == 1, F.lit("Unspecified error"))
            .when(F.col("_resultado_chamada") == 2, F.lit("Unsuccessful session setup"))
            .when(F.col("_resultado_chamada") == 3, F.lit("Internal error"))
            .when(F.col("_resultado_chamada") == 4, F.lit("Session timer timeout"))
            .when(F.col("_resultado_chamada") == 5, F.lit("CAC_REJECT"))
            .when(F.col("_resultado_chamada") == 200, F.lit("Normal end of session"))
            .when(
                (F.col("_resultado_chamada") > 200)
                & (F.col("_resultado_chamada") < 300),
                F.lit("Final Response"),
            )
            .when(
                (F.col("_resultado_chamada") >= 300)
                & (F.col("_resultado_chamada") < 400),
                F.lit("Redirection"),
            )
            .when(
                (F.col("_resultado_chamada") >= 400)
                & (F.col("_resultado_chamada") < 500),
                F.lit("Request failure"),
            )
            .when(
                (F.col("_resultado_chamada") >= 500)
                & (F.col("_resultado_chamada") < 600),
                F.lit("Server failure"),
            )
            .when(
                (F.col("_resultado_chamada") >= 600)
                & (F.col("_resultado_chamada") < 700),
                F.lit("Global failure"),
            )
            .otherwise(F.lit(None).cast(T.StringType())),
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return target_file

    @log_operation
    def transform_smp_ericsson_volte_vivo(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Transforma CDR SMP Ericsson VoLTE Vivo para o contrato padronizado do domínio.

        Objetivo da operação:
            Executar o pré-processamento específico da Vivo LTE Ericsson para separar
            metadados embutidos e, em seguida, aplicar a normalização padrão.

        Args:
            source_file: Caminho parquet com CDRs SMP Ericsson VoLTE Vivo de entrada.
            target_file: Caminho parquet de saída transformada.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Raises:
            ValueError: Se ``_informacao_rede_origem`` ou
                ``_informacao_rede_destino`` não existir ao extrair a tecnologia.

        Notes:
            - A coluna ``_numero_origem_original`` é dividida em ``;``: o
              primeiro trecho substitui ``numero_origem`` e o segundo é usado
              como ``_autenticacao``.
            - Os códigos de ``_tipo_chamada`` e ``_resultado_chamada`` conhecidos
              são convertidos para rótulos textuais. Hífens de IMEIs são
              removidos antes do pipeline comum.
            - ``_format_cell_id`` converte separadamente os identificadores
              hexadecimais de origem e destino para suas colunas de célula.
            - O pipeline comum interpreta datas com ``yyyyMMdd HHmmss``; o
                término combina ``_data`` com ``_hora_fim``. Códigos de tipo
                1/3/4 viram ``msOriginating``/``callForwarding``/``msTerminating``.
                Resultados 1/2/3 recebem os rótulos definidos no método; demais
                códigos de tipo e resultado preservam o valor recebido.
            - O pipeline comum usa ``numero_origem`` para normalização e depois
                restaura ``_numero_origem_original`` no campo original de saída,
                incluindo o trecho de autenticação que estava nessa coluna.
            - Grava Parquet com sobrescrita pelo pipeline de escrita herdado.
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
        # A autenticação está contida na coluna _numero_origem_original,
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
                "resultado_chamada",
                F.when(
                    F.col("_resultado_chamada") == "1",
                    "callHasReachedCongestionOrBusyState",
                )
                .when(
                    F.col("_resultado_chamada") == "2",
                    "callHasOnlyReachedThroughConnection",
                )
                .when(F.col("_resultado_chamada") == "3", "b-AnswerHasBeenReceived")
                .otherwise(F.col("_resultado_chamada")),
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
    def transform_stfc_huawei_ngn(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Transforma registros do layout STFC Huawei NGN usando o pipeline padrão.

        Combina os campos de data e hora extraídos, converte os códigos de tipo
        e status de chamada conhecidos para rótulos textuais e delega a
        normalização restante ao pipeline comum.

        Args:
            source_file: Caminho Parquet intermediário do layout STFC Huawei NGN.
            target_file: Diretório de saída em parquet padronizado.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Notes:
            - ``data_hora`` resulta da combinação de ``_data`` com ``_hora``;
                ``data_hora_fim`` combina ``_data_fim`` com ``_hora_fim`` quando
                ambas existem. A máscara temporal é ``ddMMyyyy HHmmss``.
            - Tipos 01/02/03/04/05 viram ``intra_office``, ``incoming_office``,
                ``outgoing_office``, ``tandem`` e ``new_service``. Resultados
                00/01/02 viram ``caller party on-hook``, ``called party on-hook``
                e ``abnormal``.
            - Códigos não previstos em ``_tipo_chamada`` e ``_resultado_chamada``
              recebem o rótulo ``"unknown"``.
            - Grava Parquet com sobrescrita pelo pipeline de escrita herdado.
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
                "resultado_chamada": F.when(
                    F.col("_resultado_chamada") == "00", F.lit("caller party on-hook")
                )
                .when(
                    F.col("_resultado_chamada") == "01", F.lit("called party on-hook")
                )
                .when(F.col("_resultado_chamada") == "02", F.lit("abnormal"))
                .otherwise(F.lit("unknown")),
            }
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        self._write_parquet(df, target_file)

        return target_file

    @log_operation
    def transform_stfc_huawei_ngn_tim(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Deriva o bilhetador do arquivo e normaliza o layout STFC Huawei NGN TIM.

        Usa o primeiro trecho de ``arquivo_origem`` separado por ponto como
        ``bilhetador`` e delega as demais operações ao pipeline comum.

        Args:
            source_file: Caminho Parquet intermediário do layout Huawei NGN TIM.
            target_file: Diretório de saída em parquet padronizado.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Notes:
            Não concatena campos de data/hora nem mapeia códigos de chamada.
            A máscara temporal usada pelo pipeline é ``yyMMddHHmmss``.
            ``arquivo_origem`` deve existir antes do pré-processamento.
            Grava Parquet com sobrescrita pelo pipeline de escrita herdado.
        """

        date_time_fmt = "yyMMddHHmmss"
        df = self.spark.read.parquet(source_file)

        df = df.withColumn(
            "bilhetador", F.split(F.col("arquivo_origem"), r"\.").getItem(0)
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        self._write_parquet(df, target_file)

        return target_file

    @log_operation
    def transform_stfc_fcdr_vivo(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Transforma registros do layout STFC Vivo FCDR usando o pipeline padrão.

        Combina os campos de data e hora extraídos, converte os códigos de tipo
        e status de chamada conhecidos para rótulos textuais e delega a
        normalização restante ao pipeline comum.

        Args:
            source_file: Caminho Parquet intermediário do layout STFC Vivo FCDR.
            target_file: Diretório de saída em parquet padronizado.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho do parquet transformado em ``target_file``.

        Notes:
            - ``data_hora`` resulta da combinação de ``_data`` com ``_hora``;
                ``data_hora_fim`` combina ``_data_fim`` com ``_hora_fim`` quando
                ambas existem. A máscara temporal é ``ddMMyy HHmmss``.
            - Tipos E/S/T viram ``Entrada``/``Saída``/``Transporte``; outros
                valores resultam em nulo antes do preenchimento da chave.
            - ``_resultado_chamada`` é convertido para inteiro e valor absoluto.
                Os códigos 0 a 7 recebem rótulos individuais; 8 a 15 representam
                congestionamento interno e 20 a 25 outros congestionamentos.
                Os demais resultados ficam nulos antes do pipeline comum.
            - Grava Parquet com sobrescrita pelo pipeline de escrita herdado.
        """

        date_time_fmt = "ddMMyy HHmmss"
        df = self.spark.read.parquet(source_file)

        df = _concat_date_time(df)

        df = df.withColumn(
            "_resultado_chamada",
            F.abs(F.col("_resultado_chamada").cast(T.IntegerType())),
        )

        df = df.withColumns(
            {
                "tipo_chamada": F.when(F.col("_tipo_chamada") == "E", F.lit("Entrada"))
                .when(F.col("_tipo_chamada") == "S", F.lit("Saída"))
                .when(F.col("_tipo_chamada") == "T", F.lit("Transporte"))
                .otherwise(F.lit(None).cast(T.StringType())),
                "resultado_chamada": F.when(
                    F.col("_resultado_chamada") == 0,
                    F.lit("Desconexão prematura do assinante A"),
                )
                .when(F.col("_resultado_chamada") == 1, F.lit("Normal"))
                .when(F.col("_resultado_chamada") == 2, F.lit("Linha ocupada"))
                .when(F.col("_resultado_chamada") == 3, F.lit("Número Mudado"))
                .when(
                    F.col("_resultado_chamada") == 4, F.lit("Congestionamento interno")
                )
                .when(
                    F.col("_resultado_chamada") == 5,
                    F.lit("Assinante Livre, sem Tarifação"),
                )
                .when(
                    F.col("_resultado_chamada") == 6,
                    F.lit("Assinante livre, com Dupla Desconexão"),
                )
                .when(
                    F.col("_resultado_chamada") == 7,
                    F.lit("Número inexistente, nível vago"),
                )
                .when(
                    (F.col("_resultado_chamada") >= 8)
                    & (F.col("_resultado_chamada") <= 15),
                    F.lit("Congestionamento interno"),
                )
                .when(
                    (F.col("_resultado_chamada") >= 20)
                    & (F.col("_resultado_chamada") <= 25),
                    F.lit("Congestionamento (outros tipos)"),
                )
                .otherwise(F.lit(None).cast(T.StringType())),
            }
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        self._write_parquet(df, target_file)

        return target_file

    @log_operation
    def transform_stfc_tropico_oi(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Combina datas e classifica tipos e resultados do layout Trópico Oi.

        Args:
            source_file: Caminho Parquet intermediário de CDRs STFC Trópico Oi.
            target_file: Diretório Parquet de saída, sobrescrito na gravação.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho de saída informado em ``target_file`` após a gravação.

        Notes:
            Combina ``_data``/``_hora`` e, se presentes, ``_data_fim``/``_hora_fim``;
            o pipeline interpreta os resultados com ``ddMMyy HHmmss``.
            Converte ``_resultado_chamada`` para inteiro e valor absoluto antes
            das comparações. Tipos 0/1/2 viram ``Interna``/``Saída``/``Entrada``.
            Resultados 10/20 indicam conclusão com/sem tarifação; 31/32 indicam
            ausência de resposta/ocupação; 43/44, congestionamento; 48/49, falha
            técnica; 50 a 56 recebem os rótulos específicos definidos no método.
            Tipos e resultados não mapeados ficam nulos antes do pipeline comum,
            que normaliza e grava a saída conforme o contrato herdado.
        """
        date_time_fmt = "ddMMyy HHmmss"
        df = self.spark.read.parquet(source_file)

        df = _concat_date_time(df)

        df = df.withColumn(
            "_resultado_chamada",
            F.abs(F.col("_resultado_chamada").cast(T.IntegerType())),
        )

        df = df.withColumns(
            {
                "tipo_chamada": F.when(F.col("_tipo_chamada") == 0, F.lit("Interna"))
                .when(F.col("_tipo_chamada") == 1, F.lit("Saída"))
                .when(F.col("_tipo_chamada") == 2, F.lit("Entrada"))
                .otherwise(F.lit(None).cast(T.StringType())),
                "resultado_chamada": F.when(
                    F.col("_resultado_chamada") == "10",
                    F.lit("Chamada completada com tarifação"),
                )
                .when(
                    F.col("_resultado_chamada") == "20",
                    F.lit("Chamada completada sem tarifação"),
                )
                .when(F.col("_resultado_chamada") == "31", F.lit("Não responde"))
                .when(F.col("_resultado_chamada") == "32", F.lit("linha ocupada"))
                .when(
                    F.col("_resultado_chamada") == "43",
                    F.lit("Congestionamento a frente"),
                )
                .when(
                    F.col("_resultado_chamada") == "44",
                    F.lit("Congestionamento a frente"),
                )
                .when(F.col("_resultado_chamada") == "48", F.lit("Falha tecnica"))
                .when(F.col("_resultado_chamada") == "49", F.lit("Falha tecnica"))
                .when(
                    F.col("_resultado_chamada") == "50",
                    F.lit("Desistencia pela origem"),
                )
                .when(
                    F.col("_resultado_chamada") == "51",
                    F.lit("Assinante com Defeito ou Fora de Serviço"),
                )
                .when(
                    F.col("_resultado_chamada") == "52",
                    F.lit("Acesso Barrado ou chamada rejeitada"),
                )
                .when(
                    F.col("_resultado_chamada") == "53",
                    F.lit("Evento não especificado"),
                )
                .when(
                    F.col("_resultado_chamada") == "54",
                    F.lit("Assinante com número mudado"),
                )
                .when(
                    F.col("_resultado_chamada") == "55",
                    F.lit("Chamada não tarifada, sem o sinal de atendimento"),
                )
                .when(
                    F.col("_resultado_chamada") == "56",
                    F.lit("Acesso inesistente ou número vago"),
                )
                .otherwise(F.lit(None).cast(T.StringType())),
            }
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        self._write_parquet(df, target_file)

        return target_file

    @log_operation
    def transform_stfc_7n_oi(self, source_file: str, target_file: str, **kwargs) -> str:
        """Combina datas e classifica resultados de chamadas do layout 7N Oi.

        Args:
            source_file: Caminho Parquet intermediário de CDRs STFC 7N Oi.
            target_file: Diretório Parquet de saída, sobrescrito na gravação.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho de saída informado em ``target_file`` após a gravação.

        Notes:
            Combina ``_data``/``_hora`` e, se presentes, ``_data_fim``/``_hora_fim``;
            a máscara temporal é ``ddMMyyyy HHmmss``. Resultados 10, 31, 32, 44
            e 48 indicam conclusão, ausência de resposta, ocupação,
            congestionamento e falha; 50 a 53 viram ``Outros`` e 99 vira
            ``Não identificado``. Demais resultados ficam nulos antes do
            pipeline comum. Não cria nem mapeia ``tipo_chamada`` nesta etapa.
        """
        date_time_fmt = "ddMMyyyy HHmmss"
        df = self.spark.read.parquet(source_file)

        df = _concat_date_time(df)

        df = df.withColumn(
            "resultado_chamada",
            F.when(
                F.col("_resultado_chamada") == "10",
                F.lit("Chamada completada"),
            )
            .when(F.col("_resultado_chamada") == "31", F.lit("Não responde"))
            .when(F.col("_resultado_chamada") == "32", F.lit("linha ocupada"))
            .when(
                F.col("_resultado_chamada") == "44",
                F.lit("Congestionamento"),
            )
            .when(F.col("_resultado_chamada") == "48", F.lit("Falha"))
            .when(
                F.col("_resultado_chamada").isin("50", "51", "52", "53"),
                F.lit("Outros"),
            )
            .when(
                F.col("_resultado_chamada") == "99",
                F.lit("Não identificado"),
            )
            .otherwise(F.lit(None).cast(T.StringType())),
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        self._write_parquet(df, target_file)

        return target_file

    @log_operation
    def transform_stfc_axe_claro(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Combina horários e classifica tipos e resultados do layout AXE Claro.

        Args:
            source_file: Caminho Parquet intermediário de CDRs STFC AXE Claro.
            target_file: Diretório Parquet de saída, sobrescrito na gravação.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho de saída informado em ``target_file`` após a gravação.

        Notes:
            Início e término usam a mesma ``_data``, combinada com ``_hora`` e
            ``_hora_fim``; não há ajuste de virada de dia. A máscara é
            ``yyMMdd HHmmss``. Tipos 01 a 0B recebem os rótulos POTS, RDSI,
            redirecionamento, procedimento, evento e serviço definidos no método;
            outros tipos ficam nulos antes do pipeline comum.
            O resultado é convertido para inteiro, sem valor absoluto. Códigos
            1 a 8 e 20/21/24/26/27/28/29 recebem rótulos específicos; 4/25 e as
            faixas 9 a 19, 22 a 23 e 30 a 99 viram ``Reserva``. Os demais casos,
            inclusive nulos, viram ``Desconexão Prematura`` antes da normalização.
        """
        date_time_fmt = "yyMMdd HHmmss"
        df = self.spark.read.parquet(source_file)

        df = _concat_date_time(
            df,
            start_date="_data",
            start_time="_hora",
            stop_date="_data",
            stop_time="_hora_fim",
        )

        df = df.withColumn(
            "tipo_chamada",
            F.when(F.col("_tipo_chamada") == "01", F.lit("Chamada POTS efetiva"))
            .when(F.col("_tipo_chamada") == "02", F.lit("Chamada POTS inefetiva"))
            .when(F.col("_tipo_chamada") == "03", F.lit("Chamada RDSI efetiva"))
            .when(F.col("_tipo_chamada") == "04", F.lit("Chamada RDSI inefetiva"))
            .when(
                F.col("_tipo_chamada") == "05", F.lit("Chamada redirecionada efetiva")
            )
            .when(
                F.col("_tipo_chamada") == "06", F.lit("Chamada redirecionada inefetiva")
            )
            .when(F.col("_tipo_chamada") == "07", F.lit("Procedimento de assinante"))
            .when(
                F.col("_tipo_chamada") == "08", F.lit("Evento não relativo a chamada")
            )
            .when(
                F.col("_tipo_chamada") == "09", F.lit("Comando de serviço de assinante")
            )
            .when(F.col("_tipo_chamada") == "0A", F.lit("Chamada RDSI-E efetiva"))
            .when(F.col("_tipo_chamada") == "0B", F.lit("Chamada RDSI-E inefetiva"))
            .otherwise(F.lit(None).cast(T.StringType())),
        )

        df = df.withColumn(
            "_resultado_chamada", F.col("_resultado_chamada").cast(T.IntegerType())
        ).withColumn(
            "resultado_chamada",
            F.when(
                F.col("_resultado_chamada") == 1, F.lit("Assinante Livre com Tarifação")
            )
            .when(F.col("_resultado_chamada") == 2, F.lit("Assinante Ocupado"))
            .when(
                F.col("_resultado_chamada") == 3, F.lit("Assinante com Número Mudado")
            )
            .when(F.col("_resultado_chamada") == 4, F.lit("Reserva"))
            .when(
                F.col("_resultado_chamada") == 5, F.lit("Assinante Livre com Tarifação")
            )
            .when(
                F.col("_resultado_chamada") == 6,
                F.lit("Assinante Livre Com Tarifação Retenção Ass. B"),
            )
            .when(F.col("_resultado_chamada") == 7, F.lit("Número inexistente"))
            .when(F.col("_resultado_chamada") == 8, F.lit("Número com defeito"))
            .when(
                (F.col("_resultado_chamada") >= 9)
                & (F.col("_resultado_chamada") <= 19),
                F.lit("Reserva"),
            )
            .when(
                F.col("_resultado_chamada") == 20,
                F.lit("Temporização na Entrada  ( CO0 )"),
            )
            .when(
                F.col("_resultado_chamada") == 21,
                F.lit("Falha  de Sinalização na Entrada  ( CO0 )"),
            )
            .when(
                (F.col("_resultado_chamada") >= 22)
                & (F.col("_resultado_chamada") <= 23),
                F.lit("Reserva"),
            )
            .when(
                F.col("_resultado_chamada") == 24,
                F.lit("Congestionamento no Destino ( CO2 )"),
            )
            .when(F.col("_resultado_chamada") == 25, F.lit("Reserva"))
            .when(
                F.col("_resultado_chamada") == 26,
                F.lit("Congestionamento Interno   ( CO1 )"),
            )
            .when(
                F.col("_resultado_chamada") == 27,
                F.lit("Falha Interna na Central   ( CO1 )"),
            )
            .when(
                F.col("_resultado_chamada") == 28,
                F.lit("Temporização na Saída   ( CO3 )"),
            )
            .when(
                F.col("_resultado_chamada") == 29,
                F.lit("Falha de Sinalização na Saída (CO3 )"),
            )
            .when(
                (F.col("_resultado_chamada") >= 30)
                & (F.col("_resultado_chamada") <= 99),
                F.lit("Reserva"),
            )
            .otherwise(F.lit("Desconexão Prematura")),
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        self._write_parquet(df, target_file)

        return target_file

    @log_operation
    def transform_stfc_pcl_claro(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Combina datas e prepara tipo e resultado de chamadas PCL Claro.

        Args:
            source_file: Caminho Parquet intermediário de CDRs STFC PCL Claro.
            target_file: Diretório Parquet de saída, sobrescrito na gravação.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho de saída informado em ``target_file`` após a gravação.

        Notes:
            Combina ``_data``/``_hora`` e, se presentes, ``_data_fim``/``_hora_fim``;
            a máscara é ``yyyyMMdd HHmmss``. ``tipo_chamada`` recebe
            ``_tipo_chamada`` convertido para string, sem mapear seus códigos.
            Somente o resultado ``"3"`` vira ``b-AnswerHasBeenReceived``;
            demais valores ficam nulos antes do pipeline comum e da gravação.
        """
        date_time_fmt = "yyyyMMdd HHmmss"
        df = self.spark.read.parquet(source_file)

        df = _concat_date_time(df)

        # Preserva os códigos de tipo recebidos, sem atribuir rótulos de domínio.
        df = df.withColumn("tipo_chamada", F.col("_tipo_chamada").cast(T.StringType()))

        df = df.withColumn(
            "resultado_chamada",
            F.when(
                F.col("_resultado_chamada") == "3", F.lit("b-AnswerHasBeenReceived")
            ).otherwise(F.lit(None).cast(T.StringType())),
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        self._write_parquet(df, target_file)

        return target_file

    @log_operation
    def transform_stfc_tropico_claro(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Combina datas e classifica chamadas do layout Trópico Claro.

        Args:
            source_file: Caminho Parquet intermediário de CDRs STFC Trópico Claro.
            target_file: Diretório Parquet de saída, sobrescrito na gravação.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho de saída informado em ``target_file`` após a gravação.

        Notes:
            Combina ``_data``/``_hora`` e, se presentes, ``_data_fim``/``_hora_fim``;
            a máscara é ``ddMMyyyy HHmmss``. Apenas o tipo ``"04"`` vira
            ``tandem``; os demais ficam nulos antes do pipeline comum.
            Resultados 003/006 indicam atendimento e desligamento; 013 a 033
            recebem os rótulos específicos definidos no método; 102/104 indicam
            ausência de conversação e 201/202 viram ``fatia da chamada``.
            Qualquer resultado não mapeado vira ``unknown``. As comparações
            usam strings, preservando a importância dos zeros iniciais.
        """
        date_time_fmt = "ddMMyyyy HHmmss"
        df = self.spark.read.parquet(source_file)

        df = _concat_date_time(df)

        df = df.withColumn(
            "tipo_chamada",
            F.when(F.col("_tipo_chamada") == "04", F.lit("tandem"))
            .otherwise(F.lit(None).cast(T.StringType()))
            .cast(T.StringType()),
        )

        df = df.withColumn(
            "resultado_chamada",
            F.when(
                F.col("_resultado_chamada") == "003",
                F.lit("chamada com atendimento e desligamento lado A ou B"),
            )
            .when(
                F.col("_resultado_chamada") == "006",
                F.lit("chamada com atendimento e desligamento lado A ou B"),
            )
            .when(
                F.col("_resultado_chamada") == "013", F.lit("assinante B não atendeu")
            )
            .when(F.col("_resultado_chamada") == "014", F.lit("assinante B ocupado"))
            .when(F.col("_resultado_chamada") == "015", F.lit("número mudado"))
            .when(
                F.col("_resultado_chamada") == "016",
                F.lit("assinante B fora de serviço"),
            )
            .when(F.col("_resultado_chamada") == "017", F.lit("unknown destination"))
            .when(F.col("_resultado_chamada") == "018", F.lit("denied acess"))
            .when(F.col("_resultado_chamada") == "019", F.lit("route failure"))
            .when(F.col("_resultado_chamada") == "020", F.lit("congestion"))
            .when(F.col("_resultado_chamada") == "021", F.lit("tecnical fail"))
            .when(F.col("_resultado_chamada") == "022", F.lit("dest congestion"))
            .when(F.col("_resultado_chamada") == "023", F.lit("signal"))
            .when(F.col("_resultado_chamada") == "024", F.lit("controller restart"))
            .when(
                F.col("_resultado_chamada") == "025", F.lit("application intervation")
            )
            .when(
                F.col("_resultado_chamada") == "026",
                F.lit("calling party idle timeout"),
            )
            .when(F.col("_resultado_chamada") == "027", F.lit("calling party abandon"))
            .when(F.col("_resultado_chamada") == "028", F.lit("internal error"))
            .when(F.col("_resultado_chamada") == "029", F.lit("unmapped event"))
            .when(
                F.col("_resultado_chamada") == "030",
                F.lit("error application intervation"),
            )
            .when(F.col("_resultado_chamada") == "031", F.lit("demais causas"))
            .when(F.col("_resultado_chamada") == "032", F.lit("normal disconnection"))
            .when(F.col("_resultado_chamada") == "033", F.lit("erro atuação aplicação"))
            .when(
                F.col("_resultado_chamada") == "102",
                F.lit("chamada que não atingiu a fase de conversação"),
            )
            .when(
                F.col("_resultado_chamada") == "104",
                F.lit("chamada que não atingiu a fase de conversação"),
            )
            .when(F.col("_resultado_chamada") == "201", F.lit("fatia da chamada"))
            .when(F.col("_resultado_chamada") == "202", F.lit("fatia da chamada"))
            .otherwise(F.lit("unknown")),
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        self._write_parquet(df, target_file)

        return target_file

    @log_operation
    def transform_stfc_pit_claro(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Recorta timestamps e deriva o sentido de chamadas do layout PIT Claro.

        Args:
            source_file: Caminho Parquet intermediário de CDRs STFC PIT Claro.
            target_file: Diretório Parquet de saída, sobrescrito na gravação.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho de saída informado em ``target_file`` após a gravação.

        Notes:
            ``data_hora`` e ``data_hora_fim`` recebem os 14 primeiros caracteres
            de ``_data_hora`` e ``_data_hora_fim``; a máscara é ``yyyyMMddHHmmss``.
            A prestadora de origem ``BRAEBT`` produz ``Originating``; qualquer
            outro valor, inclusive nulo, produz ``Terminating``.
            Não classifica o resultado da chamada: delega as demais operações
            ao pipeline comum e à gravação herdada.
        """
        date_time_fmt = "yyyyMMddHHmmss"
        df = self.spark.read.parquet(source_file)

        df = df.withColumns(
            {
                "data_hora": F.substring(F.col("_data_hora"), 1, 14),
                "data_hora_fim": F.substring(F.col("_data_hora_fim"), 1, 14),
            }
        )

        # Tipos de chamada Originada/Terminada são baseados na prestadora de origem
        df = df.withColumn(
            "tipo_chamada",
            F.when(F.col("_prestadora_origem") == "BRAEBT", F.lit("Originating"))
            .otherwise(F.lit("Terminating").cast(T.StringType()))
            .cast(T.StringType()),
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        self._write_parquet(df, target_file)

        return target_file

    @log_operation
    def transform_stfc_ss8bf_claro(
        self, source_file: str, target_file: str, **kwargs
    ) -> str:
        """Recorta referências e classifica chamadas do layout SS8BF Claro.

        Args:
            source_file: Caminho Parquet intermediário de CDRs STFC SS8BF Claro.
            target_file: Diretório Parquet de saída, sobrescrito na gravação.
            **kwargs: Opções adicionais aceitas, mas não utilizadas.

        Returns:
            str: Caminho de saída informado em ``target_file`` após a gravação.

        Notes:
            ``data_hora`` e ``data_hora_referencia`` recebem os 19 primeiros
            caracteres de ``_data_hora`` e ``_referencia``; ``referencia`` recebe
            8 caracteres a partir da posição 52 de ``_referencia``. A máscara
            temporal é ``yyyy-MM-dd'T'HH:mm:ss``. Resultados 0 a 9 recebem os
            rótulos de conclusão/falha definidos no método; demais valores
            ficam nulos antes do pipeline comum.
            A origem 900 tem prioridade e produz ``Originating``. Para origem
            901 e destino 902/903, encaminhamento não nulo produz ``Forwarding``;
            sem encaminhamento, 902 produz ``Terminating`` e 903, ``Transit``.
            Demais combinações viram ``Unknown``. String vazia em
            ``_numero_encaminhado`` conta como encaminhamento, pois o teste
            verifica apenas nulidade. O número de destino não é substituído.
        """
        date_time_fmt = "yyyy-MM-dd'T'HH:mm:ss"
        df = self.spark.read.parquet(source_file)

        df = df.withColumns(
            {
                "data_hora": F.substring(F.col("_data_hora"), 1, 19),
                "data_hora_referencia": F.substring(F.col("_referencia"), 1, 19),
                "referencia": F.substring(F.col("_referencia"), 52, 8),
            }
        )

        df = df.withColumn(
            "resultado_chamada",
            F.when(F.col("_resultado_chamada") == "0", "Call was completed")
            .when(
                F.col("_resultado_chamada") == "1",
                "Call was not completed due to called party busy",
            )
            .when(
                F.col("_resultado_chamada") == "2",
                "Call was not completed due to invalid dialed number",
            )
            .when(
                F.col("_resultado_chamada") == "3",
                "Call was not completed due to lack of available lines/trunks to complete the call",
            )
            .when(
                F.col("_resultado_chamada") == "4",
                "Call was not completed due to calling party aborting the call prior to answer",
            )
            .when(
                F.col("_resultado_chamada") == "5",
                "Call was not completed due to called party not answering the call",
            )
            .when(
                F.col("_resultado_chamada") == "6",
                "Call was not completed due to a network problem",
            )
            .when(
                F.col("_resultado_chamada") == "7",
                "Call was not completed due to unknown reasons",
            )
            .when(
                F.col("_resultado_chamada") == "8",
                "Call was not completed due to no subscriber account",
            )
            .when(
                F.col("_resultado_chamada") == "9",
                "Call was not completed due to unauthorized subscriber",
            )
            .cast(T.StringType()),
        )

        forwarding = F.col("_numero_encaminhado").isNotNull().cast("boolean")
        originating = F.col("_identificador_origem")
        terminating = F.col("_identificador_destino")

        df = df.withColumn(
            "tipo_chamada",
            F.when(
                originating == "900",
                "Originating",
            )
            .when(
                (originating == "901") & terminating.isin("902", "903") & forwarding,
                "Forwarding",
            )
            .when(
                (originating == "901") & (terminating == "902") & (forwarding == False),
                "Terminating",
            )
            .when(
                (originating == "901") & (terminating == "903") & (forwarding == False),
                "Transit",
            )
            .otherwise("Unknown"),
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)
        self._write_parquet(df, target_file)

        return target_file
