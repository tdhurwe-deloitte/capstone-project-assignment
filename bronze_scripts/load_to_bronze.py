from pyspark.sql.functions import current_timestamp, lit

from config.spark_config import get_spark_session
from utils.snowflake_loader import load_dataframe_to_snowflake

FILES = {
    "BRONZE_SALES": [
        "datasets/sales/Sales Data 2020.csv",
        "datasets/sales/Sales Data 2021.csv",
        "datasets/sales/Sales Data 2022.csv",
    ],
    "BRONZE_RETURNS": [
        "datasets/returns/Returns Data.csv",
    ],
    "BRONZE_CUSTOMER": [
        "datasets/lookups/Customer Lookup.csv",
    ],
    "BRONZE_PRODUCT": [
        "datasets/lookups/Product Lookup.csv",
    ],
    "BRONZE_PRODUCT_CATEGORY": [
        "datasets/lookups/Product Categories Lookup.csv",
    ],
    "BRONZE_PRODUCT_SUBCATEGORY": [
        "datasets/lookups/Product Subcategories Lookup.csv",
    ],
    "BRONZE_TERRITORY": [
        "datasets/lookups/Territory Lookup.csv",
    ],
    "BRONZE_CALENDAR": [
        "datasets/lookups/Calendar Lookup.csv",
    ],
}


def process_file(spark, table_name, file_path):

    print(f"\nLoading: {file_path}")

    df = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    )

    df = df.withColumn("LOAD_TIMESTAMP", current_timestamp()).withColumn(
        "SOURCE_FILE", lit(file_path)
    )
    df = df.toDF(*[c.upper() for c in df.columns])
    pandas_df = df.toPandas()

    load_dataframe_to_snowflake(
    pandas_df,
    database="CAPSTONE_BRONZE_DB",
    schema="RAW",
    table_name=table_name
)


def main():

    spark = get_spark_session()

    for table_name, file_list in FILES.items():

        for file_path in file_list:

            process_file(
                spark,
                table_name,
                file_path,
            )

    spark.stop()


if __name__ == "__main__":
    main()
