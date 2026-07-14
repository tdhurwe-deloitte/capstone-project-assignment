from pyspark.sql import functions as F

from config.spark_config import get_spark_session

from utils.snowflake_reader import read_table
from utils.snowflake_loader import load_dataframe_to_snowflake



def transform_customer():

    spark = get_spark_session()


    customer = spark.createDataFrame(
        read_table(
            database="CAPSTONE_BRONZE_DB",
            schema="RAW",
            table_name="BRONZE_CUSTOMER"
        )
    )


    customer = customer.toDF(
        *[
            c.upper()
            for c in customer.columns
        ]
    )


    # Remove duplicate customers

    customer = customer.dropDuplicates(
        ["CUSTOMERKEY"]
    )


    # Remove invalid email records

    customer = customer.filter(
        F.col("EMAILADDRESS").isNotNull()
    )


    # Replace missing income values

    customer = customer.fillna(
        {
            "ANNUALINCOME":0
        }
    )


    load_dataframe_to_snowflake(

        customer.toPandas(),

        database="CAPSTONE_SILVER_DB",

        schema="CURATED",

        table_name="SILVER_CUSTOMER"
    )


if __name__ == "__main__":

    transform_customer()