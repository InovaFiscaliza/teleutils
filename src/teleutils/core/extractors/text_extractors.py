"""Extração de CDRs delimitados ou de largura fixa para Parquet intermediário.

Este módulo lê arquivos com Spark, seleciona colunas ou recorta campos de
largura fixa conforme um contrato ``CDRTextSchema`` e persiste o resultado em
Parquet. O catálogo de layouts é definido em
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
    ...     cdr_type="stfc"
    ... )
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from teleutils._logging import format_source_file_for_log, log_operation
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
        operator: str = "",
        cdr_type: str = "",
    ) -> str:
        """Extrai campos e metadados de um layout CDR e grava Parquet intermediário.

        Fluxo de processamento:
            1. Resolve ``cdr_schema`` no catálogo da instância.
            2. Lê linhas via RDD ou CSV com as opções do contrato.
            3. Recorta campos de largura fixa ou valida e seleciona colunas por
               posição, atribuindo os nomes de ``column_names``.
            4. Adiciona ``esquema``, ``prestadora``, ``tipo_cdr`` e
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
            operator: Valor literal de ``prestadora``. Se vazio, utiliza o
                terceiro segmento a partir do fim do caminho retornado por
                ``input_file_name``.
            cdr_type: Valor literal de ``tipo_cdr``. Se vazio, utiliza o segundo
                segmento a partir do fim do mesmo caminho.

        Returns:
            str: Caminho do diretório Parquet persistido em ``target_file``.

        Raises:
            TypeError: Se ``cdr_schema`` não for uma string.
            ValueError: Se ``source_file`` for uma lista vazia;
                se ``cdr_schema`` não for uma chave válida de
                ``self.schemas`` ou, no ramo sem ``column_sizes``, se o maior
                índice requerido for maior ou igual à quantidade de colunas
                lidas. Essa verificação não é realizada no ramo de largura fixa.

        Notes:
            Na leitura CSV, ``inferSchema=False`` desativa a inferência de tipos,
            ``schema.schema`` é repassado ao leitor e espaços iniciais e finais
            são ignorados. No ramo ``read_rdd``, cada linha vira uma coluna
            ``value``; delimitador, cabeçalho e schema Spark não são aplicados.

            A leitura via RDD contorna falhas de interpretação de caminhos como
            URI pelo leitor CSV, relatadas no ambiente de origem para nomes de
            arquivos que contêm ``@``. O contrato deve definir ``read_rdd=True``
            nesses casos; o extrator não detecta o caractere nem alterna a leitura
            automaticamente.

            Com ``column_sizes``, os campos são recortados da coluna ``value``
            por ``substring``. Os índices seguem a convenção dessa função
            (primeiro caractere na posição 1), sem ajuste ou validação de largura.
            Se ``fill_char`` não estiver vazio, o padrão ``fill_char + '+$'``
            remove o preenchimento final; seu conteúdo é usado como expressão
            regular, sem escape. Em seguida, ``trim`` remove espaços das bordas.

            Sem ``column_sizes``, a seleção usa posições de coluna baseadas em
            zero, não nomes de origem, para suportar cabeçalhos não confiáveis.
            Apenas os campos selecionados ou recortados seguem para a saída,
            junto dos metadados. Não há conversão adicional de tipos dos campos.

            ``esquema`` recebe a chave ``cdr_schema``. ``arquivo_origem`` recebe
            o último segmento de ``input_file_name``, decodificado por
            ``url_decode``. Os segmentos são separados por ``/``. Não há
            alternativa para ausência de informação de arquivo no ramo RDD;
            os metadados derivados dependem do valor fornecido pelo Spark.

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
            as listas são unidas por vírgulas; no CSV, são repassadas ao leitor.
            Listas vazias são rejeitadas com ``ValueError`` antes da resolução
            do contrato e de qualquer leitura. O decorador registra a falha sem
            acessar o primeiro elemento de uma lista vazia.
        """

        if isinstance(source_file, list) and not source_file:
            raise ValueError("source_file deve conter ao menos um caminho de entrada.")

        schema = resolve_cdr_schema(self.schemas, cdr_schema)

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
            if isinstance(source_file, list):
                source_file = ",".join(source_file)
            rdd = self.spark.sparkContext.textFile(source_file)
            df = rdd.map(lambda x: (x,)).toDF(["value"])
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
            df = df.select(*columns_expressions)
        else:
            # Valida se todos os índices solicitados existem no DataFrame lido.
            logger.info("Validando índices de coluna para o esquema '%s'", schema.name)
            max_index = max(schema.column_indices)
            if max_index >= len(df.columns):
                raise ValueError(
                    f"Schema '{schema.name}' requer coluna no índice {max_index}, "
                    f"mas o arquivo possui apenas {len(df.columns)} colunas.\n"
                    f"Verifique se o delimitador '{schema.delimiter}' está correto "
                    f"para o arquivo: {source_file_log}\n"
                    f"Índices solicitados: {schema.column_indices}\n"
                    f"Colunas disponíveis: {list(enumerate(df.columns))}\n"
                    f"Configuração do schema: {schema!r}"
                )

            logger.info(
                "Selecionando e renomeando colunas conforme o esquema '%s'", schema.name
            )
            # A seleção por índice preserva compatibilidade com layouts sem cabeçalho
            # estável, onde nomes de coluna originais não são confiáveis.
            columns_to_keep = [
                F.col(df.columns[index]).alias(column_name)
                for index, column_name in zip(
                    schema.column_indices,
                    schema.column_names,
                )
            ]
            df = df.select(*columns_to_keep)

        if operator:
            operator_expression = F.lit(operator)
        else:
            operator_expression = F.element_at(F.split(F.input_file_name(), "/"), -3)
        if cdr_type:
            cdr_type_expression = F.lit(cdr_type)
        else:
            cdr_type_expression = F.element_at(F.split(F.input_file_name(), "/"), -2)
        df = df.withColumns(
            {
                "esquema": F.lit(cdr_schema),
                "prestadora": operator_expression,
                "tipo_cdr": cdr_type_expression,
                "arquivo_origem": F.url_decode(
                    F.element_at(F.split(F.input_file_name(), "/"), -1)
                ),
            }
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

        logger.info(
            "Escrevendo DataFrame extraído para parquet: %s",
            target_file,
        )
        df.write.mode("overwrite").parquet(target_file)
        return target_file
