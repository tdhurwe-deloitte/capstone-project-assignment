from .common_imports import *

def load_calendar_dimension():

    spark = get_spark_session()

    print("Loading DIMCALENDAR...")


    sales_2020 = spark.read.csv(
        "datasets/sales/Sales Data 2020.csv",
        header=True,
        inferSchema=True
    )


    sales_2021 = spark.read.csv(
        "datasets/sales/Sales Data 2021.csv",
        header=True,
        inferSchema=True
    )


    sales_2022 = spark.read.csv(
        "datasets/sales/Sales Data 2022.csv",
        header=True,
        inferSchema=True
    )


    sales = sales_2020.union(
        sales_2021
    ).union(
        sales_2022
    )


    sales = sales.toDF(
        *[
            c.upper()
            for c in sales.columns
        ]
    )


    calendar = sales.select(

        F.to_date(
            F.col("ORDERDATE")
        ).alias("FULL_DATE")

    ).dropDuplicates()


    calendar = calendar.withColumn(

        "DATE_KEY",

        F.date_format(
            "FULL_DATE",
            "yyyyMMdd"
        ).cast("int")

    )


    calendar = calendar.withColumn(
        "YEAR",
        F.year("FULL_DATE")
    )


    calendar = calendar.withColumn(
        "QUARTER",
        F.quarter("FULL_DATE")
    )


    calendar = calendar.withColumn(
        "MONTH",
        F.month("FULL_DATE")
    )


    calendar = calendar.withColumn(
        "MONTH_NAME",
        F.date_format(
            "FULL_DATE",
            "MMMM"
        )
    )


    calendar = calendar.withColumn(
        "WEEK",
        F.weekofyear("FULL_DATE")
    )


    calendar = calendar.withColumn(
        "DAY",
        F.dayofmonth("FULL_DATE")
    )


    calendar = calendar.withColumn(
        "DAY_NAME",
        F.date_format(
            "FULL_DATE",
            "EEEE"
        )
    )


    calendar = calendar.select(

        "DATE_KEY",
        "FULL_DATE",
        "YEAR",
        "QUARTER",
        "MONTH",
        "MONTH_NAME",
        "WEEK",
        "DAY",
        "DAY_NAME"

    )


    load_dataframe_to_snowflake(

        calendar.toPandas(),

        "CAPSTONE_GOLD_DB",

        "ANALYTICS",

        "DIMCALENDAR"

    )


    print("DIMCALENDAR loaded successfully.")

if __name__ == "__main__":
    load_calendar_dimension()