from config.spark_config import get_spark_session
from utils.snowflake_loader import load_dataframe_to_snowflake


def transform_returns():

    spark = get_spark_session()


    returns = spark.read.csv(

        "datasets/returns/Returns Data.csv",

        header=True,

        inferSchema=True

    )


    returns = returns.toDF(

        *[c.upper() for c in returns.columns]

    )


    returns = returns.dropDuplicates()


    load_dataframe_to_snowflake(

        returns.toPandas(),

        "CAPSTONE_SILVER_DB",

        "CURATED",

        "SILVER_RETURNS"

    )


    print("SILVER_RETURNS loaded successfully.")


if __name__ == "__main__":

    transform_returns()