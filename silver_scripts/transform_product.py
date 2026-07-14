from pyspark.sql import functions as F

from config.spark_config import get_spark_session

from utils.snowflake_loader import load_dataframe_to_snowflake


def transform_product():

    spark = get_spark_session()

    # Read Product Lookup CSV
    product = spark.read.csv(
        "datasets/lookups/Product Lookup.csv",
        header=True,
        inferSchema=True
    )

    # Convert column names to uppercase
    product = product.toDF(
        *[col.upper() for col in product.columns]
    )

    # Remove duplicate products
    product = product.dropDuplicates(["PRODUCTKEY"])

    # Handle null values
    product = product.fillna({
        "PRODUCTDESCRIPTION": "UNKNOWN",
        "PRODUCTCOLOR": "UNKNOWN",
        "PRODUCTSIZE": "UNKNOWN",
        "PRODUCTSTYLE": "UNKNOWN",
        "MODELNAME": "UNKNOWN",
        "PRODUCTCOST": 0,
        "PRODUCTPRICE": 0
    })

    # Load into Snowflake
    load_dataframe_to_snowflake(
        pandas_df=product.toPandas(),
        database="CAPSTONE_SILVER_DB",
        schema="CURATED",
        table_name="SILVER_PRODUCT"
    )

    print("SILVER_PRODUCT loaded successfully.")


if __name__ == "__main__":
    transform_product()