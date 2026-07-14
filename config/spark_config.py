from pyspark.sql import SparkSession


def get_spark_session():
    spark = (
        SparkSession.builder
        .appName("Capstone Bronze Layer")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark