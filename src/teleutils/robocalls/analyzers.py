"""Análise agregada de chamadas telefônicas para detecção de padrões abusivos.

Este módulo fornece ferramentas para analisar registros CDR transformados,
agrupando-os por número originador e hora da chamada para identificar padrões
indicadores de atividade abusiva (robocalls, campanhas de telemarketing
agressivo, etc.).

O resultado da análise é um DataFrame agregado com métricas sobre cada
combinação (numero_de_a_formatado, hora_da_chamada), incluindo:

- Quantidade total de chamadas;
- Quantidade de chamadas curtas;
- Quantidade de chamadas encaminhadas ao correio de voz (não curtas);
- Quantidade de chamadas autenticadas;
- Quantidade de chamadas curtas autenticadas;
- Quantidade de chamadas encaminhadas ao correio de voz e autenticadas.

Essas métricas são ordenadas por número de chamadas curtas (descendente),
facilitando a identificação de padrões suspeitos: números que realizam muitas
chamadas curtas em curto espaço de tempo costumam ser indicadores de
comportamento abusivo automatizado.

Nota:
    A análise trabalha sobre dados já transformados pela camada de transformação.
    Presume que estão disponíveis os campos definidos em RoboCallsTransformer.
    Os resultados são salvos como parquet sem particionamento.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class RoboCallsAnalyzer:
    """Analisa padrões de chamadas abusivas em registros CDR transformados.

    Responsável por agregar registros de chamadas por originador (numero_de_a_formatado)
    e hora (hora_da_chamada), computando métricas que indicam atividade abusiva:

    - Total de chamadas no período/originador;
    - Quantidade de chamadas curtas (indicador de robocall);
    - Quantidade de chamadas encaminhadas ao correio de voz (não curtas);
    - Quantidade de chamadas com autenticação bem-sucedida;
    - Quantidade de chamadas curtas com autenticação bem-sucedida;
    - Quantidade de chamadas encaminhadas ao correio de voz (não curtas) com autenticação bem-sucedida.

    Os resultados são ordenados por quantidade de chamadas curtas (descendente),
    permitindo priorizar a investigação dos originadores mais suspeitos.

    Atributos:
        spark (SparkSession): Sessão Spark para operações de I/O e agregação.

    Exemplo:
        >>> analyzer = RoboCallsAnalyzer(spark)
        >>> df_analise = analyzer.analyze(
        ...     source_file="parquet/ericsson_transformed",
        ...     target_file="parquet/ericsson_analyzed"
        ... )
        >>> df_analise.show(5)
        # Resultado: Top 5 originadores com mais chamadas curtas
    """

    def __init__(self, spark: SparkSession):
        """Inicializa o analisador com uma sessão Spark.

        Parâmetros:
            spark (SparkSession): Sessão Spark ativa para operações de agregação e I/O.
        """
        self.spark = spark

    def analyze(self, source_file: str, target_file: str = "") -> DataFrame:
        """Analisa e agrupa chamadas por originador e hora, detectando padrões abusivos.

        Lê um DataFrame CDR transformado e realiza agregação por
        (numero_de_a_formatado, hora_da_chamada), computando as seguintes métricas:

        - total_chamadas: Quantidade total de chamadas no grupo.
        - total_chamadas_curtas: Quantidade de chamadas com duração abaixo do
          limiar definido (alta concentração indica robocall).
        - total_chamadas_caixa_postal: Quantidade de chamadas encaminhadas ao
          correio de voz cuja duração está acima do limiar (pode indicar tentativa
          de contato com sistema automático).
        - total_chamadas_autenticadas: Quantidade de chamadas com autenticação
          bem-sucedida (chamada_autenticada == 1; valores -1 e 0 não são contados).
        - total_chamadas_curtas_autenticadas: Quantidade de chamadas curtas que
          também possuem autenticação bem-sucedida (chamada_autenticada == 1).
        - total_chamadas_caixa_postal_autenticadas: Quantidade de chamadas
          encaminhadas ao correio de voz (não curtas) com autenticação
          bem-sucedida (chamada_autenticada == 1).

        O resultado é ordenado em ordem decrescente de total_chamadas_curtas,
        colocando os padrões mais suspeitos no topo.

        Parâmetros:
            source_file (str): Caminho para o diretório que contém o parquet CDR transformado.
                Deve conter as colunas: numero_de_a_formatado, hora_da_chamada,
                referencia, chamada_curta, chamada_caixa_postal e chamada_autenticada.
            target_file (str): Caminho para o diretório que conterá o parquet de saída com os
                resultados agregados.

        Retorna:
            DataFrame: DataFrame contendo:
                - numero_de_a_formatado (str): Número originador formatado.
                - hora_da_chamada (str): Hora cheia no formato YYYYMMDDHH.
                - total_chamadas (long): Contagem total de chamadas no grupo
                  originador/hora.
                - total_chamadas_curtas (long): Contagem de chamadas com duração
                  abaixo do limiar_chamada_ofensora.
                - total_chamadas_caixa_postal (long): Contagem de chamadas não
                  curtas encaminhadas ao correio de voz.
                - total_chamadas_autenticadas (long): Contagem de chamadas com
                  chamada_autenticada == 1 (autenticação bem-sucedida).
                - total_chamadas_curtas_autenticadas (long): Contagem de chamadas
                  curtas com chamada_autenticada == 1.
                - total_chamadas_caixa_postal_autenticadas (long): Contagem de
                  chamadas não curtas encaminhadas ao correio de voz com
                  chamada_autenticada == 1.

            Ordenado por total_chamadas_curtas em ordem descendente.

        Lança:
            FileNotFoundError: Se source_file não existir.
            Exception: Qualquer erro lançado pelo Spark ao ler ou gravar parquet.

        Nota:
            - O agrupamento por (numero_de_a_formatado, hora_da_chamada) permite
              análise da concentração de chamadas por hora, identificando padrões
              temporais de abuso.
            - As métricas de autenticação contam apenas chamadas com
              chamada_autenticada == 1; falhas (-1) e não verificadas (0) são
              desconsideradas. Isso permite avaliar se os originadores suspeitos
              possuem chamadas autenticadas ou operam exclusivamente sem autenticação.
            - Os resultados são salvos como parquet plano (sem particionamento)
              para acesso rápido em consultas analíticas subsequentes.

        Exemplo:
            >>> from pyspark.sql import SparkSession
            >>> spark = SparkSession.builder.appName("Analise").getOrCreate()
            >>> analyzer = RoboCallsAnalyzer(spark)
            >>> df = analyzer.analyze(
            ...     source_file="parquet/tim_volte_transformed",
            ...     target_file="parquet/tim_volte_analyzed"
            ... )
            >>> # Visualizar top 10 originadores mais agressivos:
            >>> df.show(10)
            +---------------------+---------------+---------------+----------------------+---------------------------+---------------------------+----------------------------------+----------------------------------------+
            |numero_de_a_formatado|hora_da_chamada|total_chamadas |total_chamadas_curtas |total_chamadas_caixa_postal|total_chamadas_autenticadas|total_chamadas_curtas_autenticadas|total_chamadas_caixa_postal_autenticadas|
            +---------------------+---------------+---------------+----------------------+---------------------------+---------------------------+----------------------------------+----------------------------------------+
            |11987654321          |2026012114     |125            |98                    |22                         |0                          |0                                 |0                                       |
            |85345678901          |2026012114     |87             |71                    |15                         |1                          |1                                 |0                                       |
            |...                  |...            |...            |...                   |...                        |...                        |...                               |...                                     |
            +---------------------+---------------+---------------+----------------------+---------------------------+---------------------------+----------------------------------+----------------------------------------+
        """
        df = self.spark.read.parquet(source_file)
        df_agg = (
            df.groupBy("numero_de_a_formatado", "hora_da_chamada")
            .agg(
                F.count("referencia").alias("total_chamadas"),
                F.sum("chamada_curta").alias("total_chamadas_curtas"),
                F.sum(
                    F.when(
                        F.col("chamada_curta") == 0, F.col("chamada_caixa_postal")
                    ).otherwise(0)
                ).alias("total_chamadas_caixa_postal"),
                F.sum((F.col("chamada_autenticada") == 1).cast("int")).alias(
                    "total_chamadas_autenticadas"
                ),
                F.sum(
                    F.when(
                        (F.col("chamada_autenticada") == 1), F.col("chamada_curta")
                    ).otherwise(0)
                ).alias("total_chamadas_curtas_autenticadas"),
                F.sum(
                    F.when(
                        (
                            (F.col("chamada_autenticada") == 1)
                            & (F.col("chamada_curta") == 0)
                        ),
                        F.col("chamada_caixa_postal"),
                    ).otherwise(0)
                ).alias("total_chamadas_caixa_postal_autenticadas"),
            )
            .orderBy(F.desc("total_chamadas_curtas"))
        )
        df_agg.write.mode("overwrite").parquet(target_file)
        return self.spark.read.parquet(target_file)
