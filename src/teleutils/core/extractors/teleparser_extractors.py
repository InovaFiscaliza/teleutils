"""Módulo de extração e padronização inicial de CDRs do Teleparser.

Este módulo consolida a lógica de extração de CDRs em parquet para diferentes
fornecedores e layouts produzidos pelo Teleparser. O processo aplica mapeamento
de colunas para um schema intermediário comum, enriquece metadados de origem e
persiste o resultado para a etapa de transformação.

Responsabilidades principais:
    - Definir contratos de mapeamento por fornecedor.
    - Validar consistência de schemas configurados.
    - Ler arquivos parquet de entrada e projetar colunas padronizadas.
    - Adicionar metadados de proveniência (prestadora, tipo e arquivo).
    - Escrever parquet intermediário para consumo pelos transformadores.

Principais funcionalidades:
    - Extração para Ericsson.
    - Extração para TIM Huawei.
    - Extração para Vivo FCDR.
    - Extração para Nokia com tolerância a colunas ausentes.

Dependências relevantes:
    - pyspark.sql (DataFrame, SparkSession e funções colunares)
    - dataclasses (configuração imutável de schemas)
    - teleutils._logging.log_operation

Example:
    >>> extractor = CDRTeleparserExtractor(spark)
    >>> df = extractor.extract_cdr_ericsson("/tmp/origem", "/tmp/destino")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from teleutils._logging import log_operation

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CDRTeleparserSchema:
    """Representa o contrato de extração para um layout específico de CDR.

    A estrutura define quais colunas da origem devem ser selecionadas e como
    elas serão renomeadas no dataset intermediário. A ideia é separar configuração
    de execução: a classe ``CDRTeleparserExtractor`` apenas aplica esse contrato,
    enquanto cada instância de ``CDRTeleparserSchema`` define as regras.
    A configuração é imutável para evitar alteração acidental de regras em tempo de execução.

    Attributes:
        name:
            Nome amigável do schema (fornecedor/layout).
        column_mapping:
            Lista de pares ``(origem, destino)`` contendo o mapeamento de
            colunas da entrada para o nome padronizado intermediário.
        job_description:
            Descrição textual da operação, útil para observabilidade e logs.
    """

    name: str
    column_mapping: list[tuple[str, str]]
    job_description: str

    def __post_init__(self) -> None:
        """Valida a estrutura do mapeamento após a criação do dataclass.

        Objetivo da operação:
            Garantir que o schema contenha ao menos uma coluna e que cada item
            de ``column_mapping`` siga o formato ``(origem, destino)`` com
            valores textuais.

        Raises:
            ValueError:
                Quando ``column_mapping`` está vazio ou possui itens inválidos.

        Notes:
            - Regra de integridade: cada item deve ser uma tupla de 2 strings.
            - Anotação de manutenção: manter essa validação rígida evita falhas
              silenciosas durante o ``select`` em Spark.
        """
        if not self.column_mapping:
            raise ValueError(
                f"Schema '{self.name}': column_mapping nao pode ser vazio."
            )
        for item in self.column_mapping:
            if (
                not isinstance(item, tuple)
                or len(item) != 2
                or not isinstance(item[0], str)
                or not isinstance(item[1], str)
            ):
                raise ValueError(
                    f"Schema '{self.name}': cada item de column_mapping deve ser "
                    f"uma tupla (origem, destino) de strings. Recebido: {item!r}"
                )


class CDRTeleparserExtractor:
    """Executa extração de CDR parquet com mapeamento por fornecedor.

    A classe centraliza a leitura de dados do Teleparser, aplica projeção de
    colunas de acordo com um schema declarado e grava uma saída intermediária
    padronizada para o pipeline de transformação.

    Attributes:
        spark:
            Sessão Spark utilizada para leitura, projeção e escrita dos dados.

    Notes:
        - O dicionário ``_SCHEMAS`` concentra contratos por layout, facilitando
          manutenção e inclusão de novas integrações.
        - Métodos públicos são wrappers sem lógica adicional significativa,
          mantendo o fluxo principal em ``_extract_cdr``.
    """

    _SCHEMAS: dict[str, CDRTeleparserSchema] = {
        "ericsson": CDRTeleparserSchema(
            name="Ericsson",
            column_mapping=[
                ("networkCallReference", "referencia"),
                ("callingPartyNumber.digits", "numero_origem"),
                ("dateForStartOfCharge", "_data"),
                ("timeForStartOfCharge", "_hora"),
                ("CallModule", "tipo_chamada"),
                ("calledPartyNumber.digits", "numero_destino"),
                ("chargeableDuration", "duracao"),
                ("incomingRoute", "rota_entrada"),
                ("outgoingRoute", "rota_saida"),
            ],
            job_description="Extraindo CDR Parquet: Ericsson",
        ),
        "tim_huawei": CDRTeleparserSchema(
            name="TIM Huawei",
            column_mapping=[
                ("network-Call-Reference", "referencia"),
                ("calling-Party-Address-Generic", "_numero_origem_generico"),
                ("list-Of-Calling-Party-Address_tEL-URI", "numero_origem"),
                ("recordOpeningTime", "data_hora"),
                ("role-of-Node", "tipo_chamada"),
                ("called-Party-Address_tEL-URI", "numero_destino"),
                ("duration", "duracao"),
                ("recordType", "_tipo_cdr"),
                ("specifiedTreatmentField_incoming-Route", "rota_entrada"),
                ("specifiedTreatmentField_outgoing-Route", "rota_saida"),
            ],
            job_description="Extraindo CDR Parquet: TIM Huawei",
        ),
        "vivo_fcdr": CDRTeleparserSchema(
            name="Vivo FCDR",
            column_mapping=[
                ("callModule", "_tipo_chamada"),
                ("callingPartyNumber", "_numero_origem"),
                ("calledPartyNumber", "numero_destino"),
                ("chargeableDurat", "duracao"),
                ("dateForStartOfCharge", "_data"),
                ("timeForStartOfCharge", "_hora"),
                ("networkCallReference", "referencia"),
                ("incomingRoute", "rota_entrada"),
                ("outgoingRoute", "rota_saida"),
            ],
            job_description="Extraindo CDR Parquet: Vivo FCDR",
        ),
        "nokia": CDRTeleparserSchema(
            name="Nokia",
            column_mapping=[
                ("record_type", "tipo_chamada"),
                ("call_reference", "referencia"),
                ("call_reference_time", "data_hora_referencia"),
                ("in_channel_allocated_time", "data_hora_alocacao_canal"),
                ("calling_number", "numero_origem"),
                ("orig_calling_number", "numero_origem_original"),
                ("called_number", "numero_destino"),
                ("orig_called_number", "numero_destino_original"),
                ("connected_to_number", "numero_conectado"),
                ("forwarding_number", "numero_origem_encaminhamento"),
                ("forwarded_to_number", "numero_destino_encaminhamento"),
                ("orig_mcz_duration", "_duracao_orig_mcz"),
                ("term_mcz_duration", "_duracao_term_mcz"),
                ("forw_mcz_duration", "_duracao_forw_mcz"),
                ("roam_mcz_duration", "_duracao_roam_mcz"),
                ("iaz_duration", "_duracao_iaz"),
                ("oaz_duration", "_duracao_oaz"),
                ("chargeable_duration", "_duracao_tarifavel"),
                ("char_band_duration", "_duracao_banda_tarifavel"),
                ("in_circuit_group", "rota_entrada"),
                ("out_circuit_group", "rota_saida"),
            ],
            job_description="Extraindo CDR Parquet: Nokia",
        ),
    }

    def __init__(self, spark: SparkSession) -> None:
        """Inicializa o extrator com sessão Spark ativa.

        Args:
            spark: Sessão Spark compartilhada pelo pipeline de extração.

        Notes:
            O uso de uma única sessão favorece consistência operacional e
            reaproveitamento de contexto em jobs encadeados.
        """
        self.spark = spark
        # self._sc = spark.sparkContext

    def _extract_cdr(
        self,
        source_file: str,
        target_file: str,
        schema: CDRTeleparserSchema,
        ignore_missing_columns: bool = False,
        unique: bool = False,
    ) -> DataFrame:
        """Executa extração genérica conforme schema de mapeamento informado.

        Fluxo de processamento:
            1. Lê parquet de entrada.
            2. Valida presença de colunas requeridas (ou ignora, conforme flag).
            3. Aplica seleção e renomeação com base no schema.
            4. Enriquece metadados de origem a partir do caminho do arquivo.
            5. Remove duplicatas opcionalmente.
            6. Persiste parquet intermediário e relê resultado final.

        Args:
            source_file: Caminho do parquet de entrada.
            target_file: Caminho do parquet de saída intermediária.
            schema: Contrato de mapeamento a ser aplicado.
            ignore_missing_columns: Define se colunas ausentes devem ser
                toleradas (selecionando apenas as presentes).
            unique: Define se duplicatas devem ser removidas no resultado.

        Returns:
            DataFrame: DataFrame relido de ``target_file`` após escrita.

        Raises:
            ValueError:
                Quando colunas obrigatórias do schema estão ausentes e
                ``ignore_missing_columns`` é ``False``.
            AnalysisException:
                Propagada pelo Spark em erros de leitura/escrita parquet.

        Notes:
            - Regra de negócio: metadados ``prestadora``, ``tipo_cdr`` e
              ``arquivo_origem`` são derivados da hierarquia do path de entrada.
            - Efeito colateral: grava dados em ``target_file`` com overwrite.
            - Anotação de manutenção: qualquer mudança no padrão de diretórios
              de origem impacta a extração de metadados via ``input_file_name``.
        """
        # self._sc.setJobDescription(schema.job_description)

        logger.info("Lendo arquivo parquet: %s", source_file)
        df = self.spark.read.parquet(source_file)

        logger.info(
            "Parâmetro ignore_missing_columns: %s. %s colunas ausentes.",
            ignore_missing_columns,
            "Ignorando" if ignore_missing_columns else "Verificando",
        )

        missing_columns = [
            source_col
            for source_col, _ in schema.column_mapping
            if source_col not in df.columns
        ]

        if missing_columns and not ignore_missing_columns:
            raise ValueError(
                f"Schema '{schema.name}' requer colunas ausentes no parquet: "
                f"{missing_columns}. Colunas disponiveis: {df.columns}"
            )

        if missing_columns and ignore_missing_columns:
            logger.warning(
                "Colunas ausentes no parquet: %s. Selecionando apenas colunas presentes.",
                missing_columns,
            )

        # Filtrar o mapeamento para incluir apenas colunas presentes quando ignore_missing_columns=True
        filtered_column_mapping = (
            [
                (source_col, target_col)
                for source_col, target_col in schema.column_mapping
                if source_col in df.columns
            ]
            if ignore_missing_columns
            else schema.column_mapping
        )

        # Colunas com ponto representam caminhos de campo (nested) e exigem
        # escaping com crases para evitar interpretação incorreta pelo Spark SQL.
        select_expr = [
            F.col(f"`{source_col}`").alias(target_col)
            if "." in source_col
            else F.col(source_col).alias(target_col)
            for source_col, target_col in filtered_column_mapping
        ]

        df = (
            df.select(*select_expr)
            .withColumn(
                "prestadora", F.element_at(F.split(F.input_file_name(), "/"), -3)
            )
            .withColumn("tipo_cdr", F.element_at(F.split(F.input_file_name(), "/"), -2))
            .withColumn(
                "arquivo_origem", F.element_at(F.split(F.input_file_name(), "/"), -1)
            )
        )

        if unique:
            df = df.dropDuplicates()
            logger.info(
                "Parâmetro unique: %s. Removendo duplicatas.",
                unique,
            )

        if "_tipo_cdr" in df.columns:
            df = df.withColumn("tipo_cdr", F.col("_tipo_cdr")).drop("_tipo_cdr")

        logger.info("Escrevendo DataFrame extraido para parquet: %s", target_file)
        df.write.mode("overwrite").parquet(target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def extract_cdr_ericsson(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR Ericsson para parquet intermediário.

        Args:
            source_file: Caminho do parquet de entrada Ericsson.
            target_file: Caminho do parquet intermediário de saída.

        Returns:
            DataFrame: Resultado da extração relido de ``target_file``.

        Notes:
            Delega integralmente para ``_extract_cdr`` com schema Ericsson.
        """
        return self._extract_cdr(source_file, target_file, self._SCHEMAS["ericsson"])

    @log_operation
    def extract_cdr_tim_huawei(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR TIM Huawei com remoção de duplicatas.

        Args:
            source_file: Caminho do parquet de entrada TIM Huawei.
            target_file: Caminho do parquet intermediário de saída.

        Returns:
            DataFrame: Resultado da extração relido de ``target_file``.

        Notes:
            - Regra de negócio: ``unique=True`` para reduzir duplicidade de
              registros observada neste layout.
            - Ponto de manutenção: validar periodicamente o impacto dessa
              deduplicação em cenários de retentativa de ingestão.
        """
        df = self._extract_cdr(
            source_file, target_file, self._SCHEMAS["tim_huawei"], unique=True
        )
        return df

    @log_operation
    def extract_cdr_vivo_fcdr(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR Vivo FCDR para parquet intermediário.

        Args:
            source_file: Caminho do parquet de entrada Vivo FCDR.
            target_file: Caminho do parquet intermediário de saída.

        Returns:
            DataFrame: Resultado da extração relido de ``target_file``.

        Notes:
            Delega para ``_extract_cdr`` com schema Vivo FCDR sem ajustes
            adicionais de tolerância ou deduplicação.
        """
        df = self._extract_cdr(source_file, target_file, self._SCHEMAS["vivo_fcdr"])
        return df

    @log_operation
    def extract_cdr_nokia(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR Nokia com tolerância a colunas ausentes.

        Args:
            source_file: Caminho do parquet de entrada Nokia.
            target_file: Caminho do parquet intermediário de saída.

        Returns:
            DataFrame: Resultado da extração relido de ``target_file``.

        Notes:
            - Regra de negócio: ``ignore_missing_columns=True`` para acomodar
              variações de disponibilidade entre tipos de CDR Nokia.
            - Anotação de manutenção: sempre revisar logs de colunas ausentes
              para identificar mudanças de layout na origem.
        """
        df = self._extract_cdr(
            source_file,
            target_file,
            self._SCHEMAS["nokia"],
            ignore_missing_columns=True,
        )
        return df
