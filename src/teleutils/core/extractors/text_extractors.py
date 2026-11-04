"""Extração de CDRs delimitados ou de largura fixa para Parquet intermediário.

Este módulo lê arquivos com Spark, seleciona colunas ou recorta campos de
largura fixa conforme um contrato ``CDRTextSchema`` e persiste o resultado em
Parquet pelo método ``extract``; ``extract_dataframe`` retorna o plano
sem essa escrita. O catálogo de layouts é definido em
``teleutils.core.extractors.schemas.text``. A extração atribui nomes aos campos,
adiciona metadados e aplica o filtro do contrato, sem normalizar números
telefônicos ou converter datas e horas para o schema final de destino.

Principais responsabilidades:
    - Consumir contratos de mapeamento por formato via ``CDRTextSchema``.
    - Ler via CSV ou RDD de linhas de texto, conforme ``read_rdd``.
    - Validar o maior índice de coluna apenas na seleção de campos delimitados.
    - Recortar campos de largura fixa, retirar preenchimento final e espaços.
    - Uniformizar nomes de colunas e adicionar metadados de origem.
    - Aplicar filtros de registros definidos pelo contrato de entrada.
    - Persistir o DataFrame intermediário em Parquet e retornar seu caminho.

Dependências relevantes:
    - SparkSession e funções SQL do PySpark para leitura e transformação.
    - CDRTextSchema e TEXT_DEFAULT_SCHEMAS para os contratos de extração.
    - log_operation para registrar início, sucesso e falhas, relançando erros.

Notes:
    Sem ``column_sizes``, os índices de ``column_indices`` são posições de
    coluna baseadas em zero (Python-style indexing). Com ``column_sizes``, são posições de caracteres
    repassadas diretamente a ``substring``, cuja primeira posição é 1 (Spark-style indexing).

    A escrita do resultado usa modo ``overwrite``. O diretório de destino é
    portanto sobrescrito a cada execução de ``extract``. As transformações são
    avaliadas pelo Spark durante a ação de gravação; o retorno não é um DataFrame.

Example:
    >>> extrator = CDRTextExtractor(spark)
    >>> destino = extrator.extract(
    ...     source_file="dados/ngn_huawei.csv",
    ...     target_file="saida/ngn_huawei",
    ...     cdr_schema="stfc_huawei_ngn",
    ...     operator="prestadora",
    ...     service="stfc",
    ...     cdr_type="stfc"
    ... )
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from teleutils._logging import format_source_file_for_log, log_operation
from teleutils.core.extractors._extractors_utils import (
    SOURCE_PATH_COLUMN,
    add_source_metadata,
    read_text_with_source,
    validate_metadata_arguments,
)
from teleutils.core.extractors.schemas import (
    TEXT_DEFAULT_SCHEMAS,
    CDRTextSchema,
    resolve_cdr_schema,
)

logger = logging.getLogger(__name__)


class CDRTextExtractor:
    """Orquestra a extração de CDR texto/CSV para um formato intermediário.

    ``extract`` resolve a chave do contrato em ``schemas`` e executa a leitura,
    seleção ou recorte, enriquecimento, filtragem e persistência.

    A separação entre a execução e os mapeamentos em ``schemas`` permite alterar
    configurações de layout sem duplicar o processamento Spark.

    Attributes:
        spark: Sessão Spark utilizada para leitura e escrita de dados.
        schemas: Dicionário de contratos de mapeamento indexados por chave de
            fornecedor ou layout.

    Notes:
        Quando nenhum dicionário é fornecido ao construtor, ``schemas`` referencia
        o catálogo compartilhado ``TEXT_DEFAULT_SCHEMAS``, sem cópia. Alterações
        nesse dicionário são visíveis às instâncias que compartilham a referência.

        Novos contratos podem ser utilizados por ``extract`` através de suas
        chaves em ``schemas``, sem exigir um método específico de layout.
    """

    def __init__(
        self,
        spark: SparkSession,
        schemas: dict[str, CDRTextSchema] = TEXT_DEFAULT_SCHEMAS,
    ) -> None:
        """Inicializa o extrator com uma sessão Spark ativa.

        Args:
            spark: Sessão Spark a ser reutilizada nas operações de extração.
            schemas: Dicionário opcional de contratos por layout. Quando omitido,
                a instância usa ``TEXT_DEFAULT_SCHEMAS``.

        Notes:
            A sessão Spark é mantida em ``self.spark`` e o valor de ``schemas`` é
            mantido em ``self.schemas``. Nenhum arquivo é lido ou escrito durante
            a inicialização.
        """
        self.spark = spark
        self.schemas = schemas

    @log_operation
    def extract(
        self,
        source_file: str | list[str],
        target_file: str,
        cdr_schema: str,
        *,
        operator: str | None = "",
        service: str | None = "",
        cdr_type: str | None = "",
    ) -> str:
        """Extrai campos e metadados de um layout CDR e grava Parquet intermediário.

        Fluxo de processamento:
            1. Resolve ``cdr_schema`` no catálogo da instância.
            2. Lê linhas via RDD ou CSV com as opções do contrato.
            3. Recorta campos de largura fixa ou valida e seleciona colunas por
               posição, atribuindo os nomes de ``column_names``.
            4. Adiciona ``esquema``, ``prestadora``, ``servico``, ``tipo_cdr`` e
               ``arquivo_origem`` para identificar o contrato e a origem.
            5. Mantém registros que atendem ao filtro opcional ``lines_to_keep``.
            6. Grava o resultado em Parquet com modo ``overwrite``.

        Args:
            source_file: Caminho de entrada no formato esperado pelo contrato.
                Pode ser uma string ou uma lista não vazia de strings.
            target_file: Diretório de saída em formato Parquet, sobrescrito
                durante a gravação.
            cdr_schema: Chave do contrato de mapeamento aplicável ao formato de origem.
                Define as opções de leitura, as posições selecionadas, os nomes
                de saída e o filtro opcional.
            operator: Valor de ``prestadora``. Se vazio ou ``None``, utiliza
                o diretório operator de ``../operator/service/cdr_type/arquivo``.
            service: Valor de ``servico``. Se vazio ou ``None``, utiliza o
                diretório service da mesma estrutura.
            cdr_type: Valor de ``tipo_cdr``. Se vazio ou ``None``, utiliza o
                diretório cdr_type. A coluna ``_tipo_cdr`` prevalece, se existir.

        Returns:
            str: Caminho do diretório Parquet persistido em ``target_file``.

        Raises:
            TypeError: Se ``cdr_schema`` não for uma string ou um argumento
                de metadados não for uma string nem ``None``.
            ValueError: Se ``source_file`` for uma lista vazia;
                se ``cdr_schema`` não for uma chave válida de
                ``self.schemas`` ou, no ramo sem ``column_sizes``, se o maior
                índice requerido for maior ou igual à quantidade de colunas
                lidas; um argumento contiver somente espaços; ou algum metadado
                obrigatório não puder ser inferido do caminho. A verificação de
                índices não é realizada no ramo de largura fixa.
            FileNotFoundError: No ramo ``read_rdd``, se algum caminho de entrada
                não corresponder a nenhum item no filesystem.
            RuntimeError: No ramo ``read_rdd``, se a sessão Spark não possuir
                contexto Hadoop ativo.

        Notes:
            A leitura, a seleção ou o recorte, os metadados e o filtro são
            executados por ``extract_dataframe``; este método acrescenta a
            escrita em modo ``overwrite`` e o retorno do caminho de saída.

            Cada chamada deve receber dados de uma única prestadora e de um
            único tipo de CDR de entrada, mesmo quando ``source_file`` contém
            vários caminhos. Essa premissa é responsabilidade do chamador;
            não há validação de homogeneidade entre os arquivos. A classificação
            por registro de ``_tipo_cdr``, quando presente, pode variar, como
            nos registros ``aTSRecord`` e ``iBCFRecord``.

            Não há lista fechada de prestadoras, serviços ou tipos de CDR.
            Novos contratos podem ser adicionados a ``self.schemas`` em tempo
            de execução e selecionados por ``cdr_schema``. A validação dos
            valores e da consistência dos dados após a extração cabe ao usuário.
            Informações incorretas ou localização inadequada dos arquivos podem
            produzir metadados e dados inconsistentes.

            Na leitura CSV, ``inferSchema=False`` desativa a inferência de tipos,
            ``schema.schema`` é repassado ao leitor e espaços iniciais e finais
            são ignorados. No ramo ``read_rdd``, cada linha vira uma coluna
            ``value``; delimitador, cabeçalho e schema Spark não são aplicados.
            A origem de cada linha é mantida em uma coluna interna, removida
            antes da persistência.

            A leitura via RDD contorna falhas de interpretação de caminhos como
            URI pelo leitor CSV, relatadas no ambiente de origem para nomes de
            arquivos que contêm ``@``. O contrato deve definir ``read_rdd=True``
            nesses casos; o extrator não detecta o caractere nem alterna a leitura
            automaticamente.

            Com ``column_sizes``, os campos são recortados da coluna ``value``
            por ``substring``. Os índices seguem a convenção dessa função
            (primeiro caractere na posição 1), sem ajuste ou validação de largura.
            A coluna ``value`` vem de ``read_rdd=True`` ou de um schema CSV que a
            defina (por exemplo, ``schema="value string"``); o código não verifica
            sua existência antes de recortar.
            Se ``fill_char`` não estiver vazio, o padrão ``fill_char + '+$'``
            remove o preenchimento final; seu conteúdo é usado como expressão
            regular, sem escape. Em seguida, ``trim`` remove espaços das bordas.

            Sem ``column_sizes``, a seleção usa posições de coluna baseadas em
            zero, não nomes de origem, para suportar cabeçalhos não confiáveis.
            Apenas os campos selecionados ou recortados seguem para a saída,
            junto dos metadados. Não há conversão adicional de tipos dos campos.

            ``_tipo_cdr``, quando mapeada, prevalece sobre o argumento ou a
            inferência de ``tipo_cdr``, inclusive quando nula. Um log informa
            essa precedência. A coluna é removida somente depois dos filtros.

            ``esquema`` recebe a chave ``cdr_schema``. ``prestadora``, ``servico``
            e ``tipo_cdr`` usam argumentos prioritários sobre os diretórios
            ``../operator/service/cdr_type`` do arquivo efetivamente lido.
            Somente campos ausentes são inferidos e verificados por uma ação
            Spark antes da escrita. Argumentos completos dispensam a validação
            da hierarquia. ``arquivo_origem`` é o nome do arquivo, decodificado
            como URI e preservando sinais de mais literais, exceto se o contrato
            mapear uma coluna ``arquivo_origem``, que então é mantida; nenhum
            contrato do catálogo padrão a mapeia. O ramo RDD preserva
            a origem ao associar cada linha ao arquivo efetivamente lido.

            O filtro é aplicado depois da seleção e renomeação; por isso, seu
            primeiro elemento deve corresponder a um nome presente em
            ``schema.column_names``. Os valores especiais ``"is not null"`` e
            ``"is null"`` testam nulidade. Qualquer outro valor exige igualdade
            com um literal e não mantém nulos na coluna filtrada.

            A gravação executa o plano distribuído e persiste o resultado antes
            do retorno. O decorador ``log_operation`` registra início, sucesso
            e falhas, relançando as exceções sem tratamento adicional.

            Manutenção: ``source_file`` aceita um caminho ou uma lista de
            caminhos, conforme a assinatura ``str | list[str]``. No ramo RDD,
            arquivos são enumerados pelo filesystem Hadoop e seus RDDs unidos;
            no CSV, os caminhos são repassados ao leitor.
            Listas vazias são rejeitadas com ``ValueError`` antes da resolução
            do contrato e de qualquer leitura. O decorador registra a falha sem
            acessar o primeiro elemento de uma lista vazia.
        """

        df = self.extract_dataframe(
            source_file,
            cdr_schema,
            operator=operator,
            service=service,
            cdr_type=cdr_type,
        )
        logger.info("Escrevendo DataFrame extraído para parquet: %s", target_file)
        df.write.mode("overwrite").parquet(target_file)
        return target_file

    @log_operation
    def extract_dataframe(
        self,
        source_file: str | list[str],
        cdr_schema: str,
        *,
        operator: str | None = "",
        service: str | None = "",
        cdr_type: str | None = "",
    ) -> DataFrame:
        """Constrói a extração sem gravar Parquet intermediário.

        Args:
            source_file: Caminho de entrada ou lista não vazia de caminhos.
            cdr_schema: Chave do contrato de extração.
            operator: Prestadora explícita ou inferida do caminho.
            service: Serviço explícito ou inferido do caminho.
            cdr_type: Tipo explícito ou inferido; ``_tipo_cdr`` prevalece.

        Returns:
            DataFrame associado a ``self.spark``, com campos e metadados
            extraídos. Para consumo direto por um transformador, construa
            este extrator com ``transformer.spark``.

        Notes:
            Preserva as validações de ``extract``, inclusive a ação Spark
            para verificar metadados inferidos. O retorno não comprova que
            todo o dataset foi processado; falhas podem surgir na ação final.
        """
        if isinstance(source_file, list) and not source_file:
            raise ValueError("source_file deve conter ao menos um caminho de entrada.")

        schema = resolve_cdr_schema(self.schemas, cdr_schema)
        validate_metadata_arguments(operator, service, cdr_type)

        source_file_log = format_source_file_for_log(source_file)
        logger.info(
            "Lendo arquivo(s) CSV: %s com delimitador '%s' e header=%s",
            source_file_log,
            schema.delimiter,
            schema.has_header,
        )

        # read_rdd controla a leitura de linhas, independentemente de column_sizes.
        # O ramo RDD contorna falhas do leitor CSV relatadas para caminhos com "@".
        if schema.read_rdd:
            df = read_text_with_source(self.spark, source_file)
        else:
            df = self.spark.read.csv(
                source_file,
                sep=schema.delimiter,
                header=schema.has_header,
                schema=schema.schema,
                inferSchema=False,
                ignoreLeadingWhiteSpace=True,
                ignoreTrailingWhiteSpace=True,
            )
            df = df.withColumn(SOURCE_PATH_COLUMN, F.input_file_name())
        # O recorte de largura fixa exige uma coluna value produzida pela leitura.
        if schema.column_sizes:
            columns_expressions = []
            for indice, size, name in zip(
                schema.column_indices, schema.column_sizes, schema.column_names
            ):
                col_expression = F.substring(F.col("value"), indice, size)
                if schema.fill_char:
                    # fill_char entra no padrão sem escape de metacaracteres.
                    fill_pattern = f"{schema.fill_char}+$"
                    col_expression = F.regexp_replace(col_expression, fill_pattern, "")
                col_expression = F.trim(col_expression)
                columns_expressions.append(col_expression.alias(name))
            df = df.select(*columns_expressions, F.col(SOURCE_PATH_COLUMN))
        else:
            # Valida se todos os índices solicitados existem no DataFrame lido.
            logger.info("Validando índices de coluna para o esquema '%s'", schema.name)
            max_index = max(schema.column_indices)
            source_columns = [name for name in df.columns if name != SOURCE_PATH_COLUMN]
            if max_index >= len(source_columns):
                raise ValueError(
                    f"Schema '{schema.name}' requer coluna no índice {max_index}, "
                    f"mas o arquivo possui apenas {len(source_columns)} colunas.\n"
                    f"Verifique se o delimitador '{schema.delimiter}' está correto "
                    f"para o arquivo: {source_file_log}\n"
                    f"Índices solicitados: {schema.column_indices}\n"
                    f"Colunas disponíveis: {list(enumerate(source_columns))}\n"
                    f"Configuração do schema: {schema!r}"
                )

            logger.info(
                "Selecionando e renomeando colunas conforme o esquema '%s'", schema.name
            )
            # A seleção por índice preserva compatibilidade com layouts sem cabeçalho
            # estável, onde nomes de coluna originais não são confiáveis.
            columns_to_keep = [
                F.col(source_columns[index]).alias(column_name)
                for index, column_name in zip(
                    schema.column_indices,
                    schema.column_names,
                )
            ]
            df = df.select(*columns_to_keep, F.col(SOURCE_PATH_COLUMN))

        df = add_source_metadata(
            df,
            cdr_schema,
            operator=operator,
            service=service,
            cdr_type=cdr_type,
            logger=logger,
        )

        if schema.lines_to_keep is not None:
            col_name, col_value = schema.lines_to_keep
            if col_value == "is not null":
                filter_expression = F.col(col_name).isNotNull()
            elif col_value == "is null":
                filter_expression = F.col(col_name).isNull()
            else:
                filter_expression = F.col(col_name) == F.lit(col_value)
            logger.info(
                "Aplicando filtro: %s = '%s' para o esquema '%s'",
                col_name,
                col_value,
                schema.name,
            )
            df = df.filter(filter_expression)

        df = df.drop("_tipo_cdr")
        return df
