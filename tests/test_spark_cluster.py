from pyspark.sql import SparkSession


def test_spark_session_and_dataframe():
    spark = SparkSession.builder.appName("teleutils-spark-smoke-test").getOrCreate()  # type: ignore

    try:
        records = [
            (index, f"registro-{index}", index * 10, index % 2 == 0)
            for index in range(1, 11)
        ]
        df = spark.createDataFrame(records, ["id", "nome", "valor", "ativo"])

        assert len(df.columns) == 4
        assert df.count() == 10
        df.show(10, truncate=False)
    finally:
        spark.stop()
