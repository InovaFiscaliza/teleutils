from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class RoboCallsAnalyzer:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def analyze(self, source_file: str, target_file: str = "") -> DataFrame:
        df = self.spark.read.parquet(source_file)
        df_agg = (
            df.groupBy("numero_de_a_formatado", "hora_da_chamada")
            .agg(
                F.count("referencia").alias("total_chamadas"),
                F.sum("chamada_curta").alias("total_chamadas_curtas"),
                F.sum("chamada_caixa_postal").alias("total_chamadas_caixa_postal"),
                F.sum(F.greatest(F.col("chamada_autenticada"), F.lit(0))).alias(
                    "total_chamadas_autenticadas"
                ),
            )
            .orderBy(F.desc("total_chamadas_curtas"))
        )
        df_agg.write.mode("overwrite").parquet(target_file)
        return self.spark.read.parquet(target_file)
