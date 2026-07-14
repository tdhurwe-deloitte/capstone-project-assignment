from .common_imports import *


def load_customer_dimension():

    spark = get_spark_session()

    # Read Silver Customer table
    customer = spark.createDataFrame(
        read_table(
            database="CAPSTONE_SILVER_DB",
            schema="CURATED",
            table_name="SILVER_CUSTOMER"
        )
    )

    # Standardize column names
    customer = customer.toDF(
        *[c.upper() for c in customer.columns]
    )

    # Remove duplicate customers
    customer = customer.dropDuplicates(["CUSTOMERKEY"])

    # Generate sequential surrogate key
    window_spec = Window.orderBy("CUSTOMERKEY")

    customer = customer.withColumn(
        "CUSTOMER_KEY",
        F.row_number().over(window_spec)
    )

    # Rename business key
    customer = customer.withColumnRenamed(
        "CUSTOMERKEY",
        "CUSTOMERID"
    )

    # Select columns in the same order as DIMCUSTOMER table
    customer = customer.select(
        "CUSTOMER_KEY",
        "CUSTOMERID",
        "PREFIX",
        "FIRSTNAME",
        "LASTNAME",
        "EMAILADDRESS",
        "BIRTHDATE",
        "GENDER",
        "MARITALSTATUS",
        "ANNUALINCOME",
        "EDUCATIONLEVEL",
        "OCCUPATION",
        "HOMEOWNER"
    )

    # Load into Snowflake
    load_dataframe_to_snowflake(
        customer.toPandas(),
        database="CAPSTONE_GOLD_DB",
        schema="ANALYTICS",
        table_name="DIMCUSTOMER"
    )

    print("DIMCUSTOMER loaded successfully.")


def load_product_dimension():

    spark = get_spark_session()

    product = spark.createDataFrame(
        read_table(
            database="CAPSTONE_SILVER_DB", schema="CURATED", table_name="SILVER_PRODUCT"
        )
    )

    product = product.toDF(*[c.upper() for c in product.columns])

    product = product.dropDuplicates(["PRODUCTKEY"])

    product = product.withColumn("PRODUCTID", F.col("PRODUCTKEY"))

    load_dataframe_to_snowflake(
        product.toPandas(),
        database="CAPSTONE_GOLD_DB",
        schema="ANALYTICS",
        table_name="DIMPRODUCT",
    )


if __name__ == "__main__":

    load_customer_dimension()

    load_product_dimension()
