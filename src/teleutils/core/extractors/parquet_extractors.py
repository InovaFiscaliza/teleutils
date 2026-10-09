"""Módulo de extração e padronização inicial de CDRs do Teleparser.

Este módulo consolida a lógica de extração de CDRs em parquet para diferentes
fornecedores e layouts produzidos pelo Teleparser. O processo aplica mapeamento
de colunas para um schema intermediário comum, enriquece metadados de origem e
persiste o resultado para a etapa de transformação.

As definições de schema (dataclass ``CDRParquetSchema`` e os contratos
padrão por fornecedor) residem no módulo ``teleutils.core.extractors.schemas``,
mantendo aqui apenas a lógica de execução da extração.

Responsabilidades principais:
    - Ler um ou mais caminhos parquet de entrada e projetar colunas
            padronizadas conforme um schema informado.
    - Adicionar metadados de proveniência (prestadora, tipo e arquivo).
    - Escrever parquet intermediário para consumo pelos transformadores.

Principais funcionalidades:
        - Mapeamento dos layouts SMP Ericsson GSM, Ericsson VoLTE Vivo,
            Huawei VoLTE TIM e Nokia GSM definidos no catálogo padrão.
        - Leitura com união de schemas e preenchimento de campos ausentes com nulos.
        - Tratamento literal de nomes de colunas que contêm pontos.
        - Remoção opcional de duplicatas antes do ajuste final de ``tipo_cdr``.

Dependências relevantes:
    - pyspark.sql (DataFrame, SparkSession e funções colunares)
    - teleutils.core.extractors.schemas (contratos de mapeamento por fornecedor)
    - teleutils._logging.log_operation

Notes:
    ``extract`` recebe uma chave de ``PARQUET_DEFAULT_SCHEMAS`` ou do catálogo
    fornecido ao construtor. A seleção renomeia campos sem normalizar números
    telefônicos nem interpretar datas. Colunas existentes preservam seus tipos;
    campos ausentes no DataFrame lido são criados como nulos do tipo string.

    As transformações constroem o plano distribuído, materializado pela escrita
    Parquet em modo ``overwrite``. O destino é sobrescrito, sem particionamento
    explícito. O retorno é o caminho de saída, não um DataFrame.

Example:
    >>> extractor = CDRParquetExtractor(spark)
    >>> destino = extractor.extract(
    ...     "/tmp/origem", "/tmp/destino", cdr_schema="smp_ericsson_gsm"
    ... )
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql import functions as F  # type: ignore

from teleutils._logging import format_source_file_for_log, log_operation
from teleutils.core.extractors.schemas import (
    PARQUET_DEFAULT_SCHEMAS,
    CDRParquetSchema,
    resolve_cdr_schema,
)

logger = logging.getLogger(__name__)


class CDRParquetExtractor:
    """Executa extração de CDR parquet com mapeamento por fornecedor.

    A classe centraliza a leitura de dados do Teleparser, aplica projeção de
    colunas de acordo com um schema declarado e grava uma saída intermediária
    padronizada para o pipeline de transformação.

    Attributes:
        spark:
            Sessão Spark utilizada para leitura, projeção e escrita dos dados.
        schemas:
            Dicionário de contratos ``CDRParquetSchema`` disponíveis para
            extração, indexados por chave de fornecedor/layout.

    Notes:
        - Os schemas são injetados via construtor (``schemas``), permitindo
            substituir ou estender os contratos padrão sem alterar esta classe.
        - Quando o catálogo é omitido, ``schemas`` referencia o dicionário
            compartilhado ``PARQUET_DEFAULT_SCHEMAS``, sem cópia. Alterações nesse
            dicionário são visíveis às instâncias que compartilham a referência.
        - O fluxo principal está em ``extract``; novos contratos são consumidos
            por sua chave, sem necessidade de um método específico por layout.
    """

    def __init__(
        self,
        spark: SparkSession,
        schemas: dict[str, CDRParquetSchema] = PARQUET_DEFAULT_SCHEMAS,
    ) -> None:
        """Inicializa o extrator com sessão Spark ativa e schemas de mapeamento.

        Args:
            spark: Sessão Spark compartilhada pelo pipeline de extração.
            schemas: Dicionário de contratos ``CDRParquetSchema`` a utilizar.
                Quando omitido, os contratos padrão definidos em
                ``PARQUET_DEFAULT_SCHEMAS`` são adotados.

        Notes:
            A sessão e o dicionário recebidos são armazenados diretamente,
            sem criar uma nova sessão nem copiar o catálogo. Não há leitura
            ou gravação de arquivos durante a inicialização.
        """
        self.spark = spark
        self.schemas = schemas

    @log_operation
    def extract(
        self,
        source_file: str,
        target_file: str,
        cdr_schema: str,
        unique: bool = False,
    ) -> str:
        """Seleciona campos e metadados conforme a chave de um contrato Parquet.

        Fluxo de processamento:
            1. Resolve ``cdr_schema`` no catálogo da instância.
            2. Lê um caminho ou uma lista de caminhos com ``mergeSchema=true``.
            3. Seleciona e renomeia os campos mapeados, usando nulos do tipo
               string para aqueles ausentes no DataFrame lido.
            4. Adiciona a chave do contrato e os metadados do arquivo de entrada.
            5. Remove duplicatas, se ``unique`` estiver habilitado.
            6. Substitui ``tipo_cdr`` por ``_tipo_cdr``, se esta coluna existir,
               e remove ``_tipo_cdr``.
            7. Sobrescreve o destino com o Parquet intermediário.

        Args:
            source_file: Caminho do parquet de entrada. Embora anotado como
                ``str``, o código também trata uma lista não vazia de caminhos, que
                é expandida na chamada de leitura Spark.
            target_file: Caminho do parquet de saída intermediária.
            cdr_schema: Chave de ``self.schemas`` que identifica o contrato
                ``CDRParquetSchema`` e seus pares ``(origem, destino)``.
            unique: Se verdadeiro, aplica ``dropDuplicates`` considerando todas
                as colunas projetadas e os metadados, antes de substituir
                ``tipo_cdr``. O padrão é ``False``.

        Returns:
            str: Caminho do parquet persistido em ``target_file``.

        Raises:
            TypeError: Se ``cdr_schema`` não for uma string.
            ValueError: Se ``cdr_schema`` não estiver no catálogo da instância.

        Notes:
            ``mergeSchema=true`` solicita a união dos schemas dos arquivos.
            A ausência de uma coluna é verificada no DataFrame resultante,
            não separadamente em cada arquivo. Ausências são registradas em
            nível de aviso; não interrompem a extração. Não há conversão
            explícita dos tipos das colunas existentes neste método.

            A projeção preserva a ordem de ``column_mapping`` e descarta campos
            não mapeados. Nomes de origem com pontos são envolvidos em crases:
            o acesso é a uma coluna literal de nível superior, não a um campo
            aninhado. ``job_description`` não é utilizado nesta execução.

            ``esquema`` recebe a chave ``cdr_schema``. Separando
            ``input_file_name`` por ``/``, ``prestadora`` recebe o terceiro
            segmento a partir do fim, ``tipo_cdr`` o segundo e ``arquivo_origem``
            o último. O nome do arquivo não passa por decodificação de URL.
            Esses metadados substituem colunas de mesmo nome na projeção;
            o resultado depende da hierarquia dos caminhos de origem.

            Se o contrato produzir ``_tipo_cdr``, essa coluna substitui
            ``tipo_cdr`` após a deduplicação, inclusive quando seu valor é nulo.
            Não há fallback para o tipo derivado do diretório. A deduplicação
            inclui ``arquivo_origem`` e ``_tipo_cdr`` quando presente, portanto
            registros de arquivos distintos não são eliminados apenas por
            terem os mesmos campos de chamada. Não há nova deduplicação após
            substituir ``tipo_cdr`` e remover ``_tipo_cdr``; a saída ainda pode
            conter linhas iguais após esse ajuste.

            A escrita em modo ``overwrite`` materializa o plano Spark e
            sobrescreve o destino antes do retorno. ``log_operation`` registra
            início, sucesso e falhas da execução do método e relança exceções;
            não há recuperação de falhas de leitura, transformação ou escrita.
        """

        schema = resolve_cdr_schema(self.schemas, cdr_schema)

        source_file_log = format_source_file_for_log(source_file)
        logger.info("Lendo arquivo(s) parquet: %s", source_file_log)

        if isinstance(source_file, list):
            df = self.spark.read.option("mergeSchema", "true").parquet(*source_file)
        else:
            df = self.spark.read.option("mergeSchema", "true").parquet(source_file)

        missing_columns = [
            source_col
            for source_col, _ in schema.column_mapping
            if source_col not in df.columns
        ]

        if missing_columns:
            logger.warning(
                "Schema '%s': colunas ausentes no parquet: %s. "
                "Criando-as com valor nulo.",
                schema.name,
                missing_columns,
            )

        # Crases fazem o Spark interpretar pontos no nome de origem literalmente,
        # sem tratá-los como separadores de campos aninhados.
        select_expr = []
        for source_col, target_col in schema.column_mapping:
            if source_col in df.columns:
                source_expr = (
                    F.col(f"`{source_col}`") if "." in source_col else F.col(source_col)
                )
            else:
                source_expr = F.lit(None).cast("string")

            select_expr.append(source_expr.alias(target_col))

        df = df.select(*select_expr).withColumns(
            {
                "esquema": F.lit(cdr_schema),
                "prestadora": F.element_at(F.split(F.input_file_name(), "/"), -3),
                "tipo_cdr": F.element_at(F.split(F.input_file_name(), "/"), -2),
                "arquivo_origem": F.element_at(F.split(F.input_file_name(), "/"), -1),
            }
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

        return target_file
