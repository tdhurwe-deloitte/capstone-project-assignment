from config.spark_config import get_spark_session
from utils.snowflake_loader import load_dataframe_to_snowflake


def transform_territory():

    spark = get_spark_session()


    territory = spark.read.csv(

        "datasets/lookups/Territory Lookup.csv",

        header=True,

        inferSchema=True

    )


    territory = territory.toDF(

        *[c.upper() for c in territory.columns]

    )


    print("Territory columns:")
    print(territory.columns)


    territory = territory.dropDuplicates()


    load_dataframe_to_snowflake(

        territory.toPandas(),

        "CAPSTONE_SILVER_DB",

        "CURATED",

        "SILVER_TERRITORY"

    )


    print("SILVER_TERRITORY loaded successfully.")


if __name__ == "__main__":

    transform_territory()