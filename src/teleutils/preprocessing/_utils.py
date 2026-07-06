"""Módulo de funções utilitárias para pré-processamento de dados.

Este módulo foi projetado para concentrar utilitários reutilizáveis de uso
geral dentro do pacote de pré-processamento. No estado atual, a única
funcionalidade disponível é a validação de CNPJ, com suporte para execução
local e em pipelines Spark via pandas UDF.

Responsabilidades:
    - Servir como ponto central para funções utilitárias gerais do domínio.
    - Disponibilizar validação de CNPJ em memória para uso direto em Python.
    - Expor UDF pandas para validação de CNPJ em lote no Apache Spark.

Funcionalidades atualmente implementadas:
    - Verificação de formato e tamanho do CNPJ.
    - Rejeição de sequências triviais (todos os dígitos iguais).
    - Cálculo e conferência de dígitos verificadores com pesos oficiais.
    - Retorno estruturado em DataFrame com coluna booleana de validade.

Dependências relevantes:
    - pandas (DataFrame e Series)
    - pyspark.sql.functions.pandas_udf
    - pyspark.sql.types (StructType, StructField, BooleanType)

Exemplo:
    >>> validar_cnpj("11222333000181")
    True
"""

import re
from typing import Union

from pandas import DataFrame, Series
from pyspark.sql.functions import pandas_udf  # type: ignore
from pyspark.sql.types import BooleanType, StructField, StructType


def validar_cnpj(cnpj: Union[str, int]) -> bool:
    """Valida um CNPJ utilizando as regras oficiais de dígitos verificadores.

    A validação ocorre em quatro etapas principais: sanitização da entrada,
    checagem estrutural básica, descarte de sequências inválidas triviais e
    verificação dos dois dígitos finais calculados pelo algoritmo de módulo
    11 aplicado ao CNPJ.

    Args:
        cnpj: CNPJ a ser validado. Aceita valores textuais com máscara,
            valores numéricos inteiros e outras representações conversíveis
            para string.

    Returns:
        bool:
            True quando o CNPJ é estruturalmente válido e possui dígitos
            verificadores consistentes; False caso contrário.

    Notes:
        Sanitização aplicada antes da validação:
            - Conversão do valor de entrada para string.
            - Remoção de todos os caracteres não numéricos.
            - Preenchimento com zeros à esquerda até 14 dígitos.

        Entradas com mais de 14 dígitos numéricos são rejeitadas para evitar
        ambiguidades e manter aderência ao formato oficial de CNPJ.
    """
    # 1. Sanitização e validações defensivas de entrada.
    # bool é rejeitado explicitamente pois é subtipo de int em Python e
    # poderia gerar resultados inesperados (True -> "1", False -> "0").
    if cnpj is None or isinstance(cnpj, bool):
        return False

    try:
        cnpj = str(cnpj)
    except Exception:
        return False

    cnpj = re.sub(r"\D", "", cnpj)

    if not cnpj:
        return False

    if len(cnpj) > 14:
        return False

    cnpj = cnpj.zfill(14)

    # 2. Validação básica de formato e tamanho após sanitização.
    if not cnpj.isdigit() or len(cnpj) != 14:
        return False

    # 3. Elimina CNPJs com todos os números iguais (comum em geradores falsos)
    if len(set(cnpj)) == 1:
        return False

    # Função auxiliar para calcular cada dígito verificador pelo algoritmo
    # módulo 11. Mantida como função interna para preservar o escopo local da
    # regra e evitar uso indevido fora do fluxo de validação de CNPJ.
    def calcular_digito(fatia, pesos):
        soma = sum(int(num) * peso for num, peso in zip(fatia, pesos))
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    # Pesos oficiais da Receita Federal
    pesos_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    # 4. Cálculo do primeiro dígito verificador
    digito_1 = calcular_digito(cnpj[:12], pesos_1)

    # 5. Cálculo do segundo dígito verificador
    digito_2 = calcular_digito(cnpj[:13], pesos_2)

    # 6. Verificação final
    return cnpj[-2:] == f"{digito_1}{digito_2}"


# Schema de retorno do UDF Spark para validação de CNPJ.
# Anotação de manutenção: alterações no nome/tipo da coluna exigem revisão
# coordenada de transformadores e consultas que dependam de cnpj_valido.
CNPJ_RETURN_SCHEMA = StructType(
    [
        StructField("cnpj_valido", BooleanType(), True),
    ]
)


@pandas_udf(CNPJ_RETURN_SCHEMA)  # type: ignore
def spark_validar_cnpj(cnpj_series: Series) -> DataFrame:
    """Valida CNPJs em lote para uso em pipelines Apache Spark.

    Esta pandas UDF recebe uma série de CNPJs e aplica a função
    validar_cnpj elemento a elemento, retornando um DataFrame com a
    coluna booleana de validade conforme o schema declarado no módulo.

    Args:
        cnpj_series: Série pandas com valores de CNPJ a serem validados.

    Returns:
        DataFrame:
            DataFrame de uma coluna com o resultado da validação:
            - cnpj_valido (bool | None): indicador de validade do CNPJ.

    Notes:
        O processamento é vetorizado em lote (batch), reduzindo overhead de
        serialização em comparação com UDFs linha a linha e melhorando
        desempenho em grandes volumes.

        Efeito colateral indireto: por ser executada no contexto Spark, a
        função depende da serialização distribuída da lógica para workers.
    """
    # Processar em batch (vetorizado)
    results = []
    for cnpj in cnpj_series:
        results.append(validar_cnpj(cnpj))

    return DataFrame(
        results,
        columns=[
            "cnpj_valido",
        ],
    )
