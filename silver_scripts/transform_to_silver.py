from pyspark.sql.functions import col, current_timestamp, when, round
from pyspark.sql import functions as F
from config.spark_config import get_spark_session
from utils.snowflake_reader import read_table
from utils.snowflake_loader import load_dataframe_to_snowflake
from pyspark.sql.functions import col


def transform_sales():

    spark = get_spark_session()

    sales = spark.createDataFrame(
        read_table(
            database="CAPSTONE_BRONZE_DB", schema="RAW", table_name="BRONZE_SALES"
        )
    )

    sales = (
    sales
    .withColumn(
        "ORDERQUANTITY",
        col("ORDERQUANTITY").cast("int")
    )
    .withColumn(
        "PRODUCTKEY",
        col("PRODUCTKEY").cast("int")
    )
    .withColumn(
        "CUSTOMERKEY",
        col("CUSTOMERKEY").cast("int")
    )
    .withColumn(
        "TERRITORYKEY",
        col("TERRITORYKEY").cast("int")
    )
)

    product = spark.createDataFrame(
        read_table(
            database="CAPSTONE_BRONZE_DB", schema="RAW", table_name="BRONZE_PRODUCT"
        )
    )

    returns = spark.createDataFrame(
        read_table(
            database="CAPSTONE_BRONZE_DB", schema="RAW", table_name="BRONZE_RETURNS"
        )
    )

    # Remove duplicates

    sales = sales.dropDuplicates(["ORDERNUMBER", "ORDERLINEITEM"])

    # Join product details

    sales = sales.join(
        product.select("PRODUCTKEY", "PRODUCTPRICE", "PRODUCTCOST"),
        "PRODUCTKEY",
        "left",
    )

    # Revenue calculations

    sales = sales.withColumn("REVENUE", col("ORDERQUANTITY") * col("PRODUCTPRICE"))

    sales = sales.withColumn("COST", col("ORDERQUANTITY") * col("PRODUCTCOST"))

    sales = sales.withColumn("PROFIT", col("REVENUE") - col("COST"))

    sales = sales.withColumn(
        "PROFITMARGIN", round((col("PROFIT") / col("REVENUE")) * 100, 2)
    )

    sales = sales.withColumn("LOAD_TIMESTAMP", F.current_timestamp())

    pandas_df = sales.toPandas()

    load_dataframe_to_snowflake(
        pandas_df,
        database="CAPSTONE_SILVER_DB",
        schema="CURATED",
        table_name="SILVER_SALES",
    )


if __name__ == "__main__":
    transform_sales()
