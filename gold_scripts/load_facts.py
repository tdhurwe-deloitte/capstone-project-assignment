from .common_imports import *



def load_fact_sales():

    spark = get_spark_session()

    print("Loading FACTSALES...")


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


    product = spark.createDataFrame(

        read_table(
            "CAPSTONE_GOLD_DB",
            "ANALYTICS",
            "DIMPRODUCT"
        )

    )


    customer = spark.createDataFrame(

        read_table(
            "CAPSTONE_GOLD_DB",
            "ANALYTICS",
            "DIMCUSTOMER"
        )

    )


    territory = spark.createDataFrame(

        read_table(
            "CAPSTONE_GOLD_DB",
            "ANALYTICS",
            "DIMTERRITORY"
        )

    )


    calendar = spark.createDataFrame(

        read_table(
            "CAPSTONE_GOLD_DB",
            "ANALYTICS",
            "DIMCALENDAR"
        )

    )


    sales = sales.join(

        product,

        sales.PRODUCTKEY == product.PRODUCT_ID,

        "left"

    )


    sales = sales.join(

        customer,

        sales.CUSTOMERKEY == customer.CUSTOMER_ID,

        "left"

    )


    sales = sales.join(

        territory,

        sales.TERRITORYKEY == territory.TERRITORY_ID,

        "left"

    )


    sales = sales.join(

        calendar,

        F.to_date(sales.ORDERDATE) == calendar.FULL_DATE,

        "left"

    )


    sales = sales.withColumn(

        "REVENUE",

        F.col("ORDERQUANTITY") *
        F.col("PRODUCT_PRICE")

    )


    sales = sales.withColumn(

        "COST",

        F.col("ORDERQUANTITY") *
        F.col("PRODUCT_COST")

    )


    sales = sales.withColumn(

        "PROFIT",

        F.col("REVENUE") -
        F.col("COST")

    )


    sales = sales.withColumn(

        "PROFIT_MARGIN",

        (
            F.col("PROFIT") /
            F.col("REVENUE")
        ) * 100

    )


    sales = sales.select(

        "DATE_KEY",
        "CUSTOMER_KEY",
        "PRODUCT_KEY",
        "TERRITORY_KEY",
        "ORDERNUMBER",
        "ORDERLINEITEM",
        "ORDERQUANTITY",
        "REVENUE",
        "COST",
        "PROFIT",
        "PROFIT_MARGIN"

    )


    load_dataframe_to_snowflake(

        sales.toPandas(),

        "CAPSTONE_GOLD_DB",
        "ANALYTICS",
        "FACTSALES"

    )


    print("FACTSALES loaded successfully.")




def load_fact_returns():

    spark = get_spark_session()

    print("Loading FACTRETURNS...")


    returns = spark.read.csv(
    "datasets/returns/Returns Data.csv",
    header=True,
    inferSchema=True
)


    returns = returns.toDF(

        *[c.upper() for c in returns.columns]

    )


    product = spark.createDataFrame(

        read_table(

            "CAPSTONE_GOLD_DB",

            "ANALYTICS",

            "DIMPRODUCT"

        )

    )


    territory = spark.createDataFrame(

        read_table(

            "CAPSTONE_GOLD_DB",

            "ANALYTICS",

            "DIMTERRITORY"

        )

    )


    calendar = spark.createDataFrame(

        read_table(

            "CAPSTONE_GOLD_DB",

            "ANALYTICS",

            "DIMCALENDAR"

        )

    )


    returns = returns.join(

        product,

        returns.PRODUCTKEY == product.PRODUCT_ID,

        "left"

    )


    returns = returns.join(

        territory,

        returns.TERRITORYKEY == territory.TERRITORY_ID,

        "left"

    )


    returns = returns.join(

        calendar,

        F.to_date(returns.RETURNDATE) == calendar.FULL_DATE,

        "left"

    )


    returns = returns.select(

        "DATE_KEY",

        "PRODUCT_KEY",

        "TERRITORY_KEY",

        "RETURNQUANTITY"

    )


    load_dataframe_to_snowflake(

        returns.toPandas(),

        "CAPSTONE_GOLD_DB",

        "ANALYTICS",

        "FACTRETURNS"

    )


    print("FACTRETURNS loaded successfully.")





if __name__ == "__main__":

    load_fact_sales()

    load_fact_returns()