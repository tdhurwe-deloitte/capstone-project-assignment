from .common_imports import *


def load_customer_dimension():

    spark = get_spark_session()

    # Read Silver Customer table
    customer = spark.createDataFrame(
        read_table(
            database="CAPSTONE_SILVER_DB",
            schema="CURATED",
            table_name="SILVER_CUSTOMER",
        )
    )

    # Standardize column names
    customer = customer.toDF(*[c.upper() for c in customer.columns])

    # Remove duplicate customers
    customer = customer.dropDuplicates(["CUSTOMERKEY"])

    # Generate sequential surrogate key
    window_spec = Window.orderBy("CUSTOMERKEY")

    customer = customer.withColumn("CUSTOMER_KEY", F.row_number().over(window_spec))

    # Rename business key
    customer = customer.withColumnRenamed("CUSTOMERKEY", "CUSTOMERID")

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
        "HOMEOWNER",
    )

    # Load into Snowflake
    load_dataframe_to_snowflake(
        customer.toPandas(),
        database="CAPSTONE_GOLD_DB",
        schema="ANALYTICS",
        table_name="DIMCUSTOMER",
    )

    print("DIMCUSTOMER loaded successfully.")


def load_product_dimension():

    spark = get_spark_session()

    print("Loading DIMPRODUCT...")


    product = spark.createDataFrame(

        read_table(
            database="CAPSTONE_SILVER_DB",
            schema="CURATED",
            table_name="SILVER_PRODUCT"
        )

    )


    # Normalize column names
    product = product.toDF(
        *[
            c.upper()
            for c in product.columns
        ]
    )


    # Remove duplicates
    product = product.dropDuplicates(
        ["PRODUCTKEY"]
    )


    # Add surrogate key
    product = product.withColumn(
        "PRODUCT_KEY",
        F.monotonically_increasing_id()
    )


    # Select columns matching Gold table
    product = product.select(

        "PRODUCT_KEY",
        "PRODUCTKEY",
        "PRODUCTSKU",
        "PRODUCTNAME",
        "MODELNAME",
        "PRODUCTDESCRIPTION",
        "PRODUCTCOLOR",
        "PRODUCTSIZE",
        "PRODUCTSTYLE",
        "PRODUCTCOST",
        "PRODUCTPRICE"

    )


    load_dataframe_to_snowflake(

        product.toPandas(),

        database="CAPSTONE_GOLD_DB",

        schema="ANALYTICS",

        table_name="DIMPRODUCT"

    )


    print("DIMPRODUCT loaded successfully.")

def load_product_category_dimension():

    spark = get_spark_session()

    print("Loading DIMPRODUCTCATEGORY...")


    category = spark.read.csv(

        "datasets/lookups/Product Categories Lookup.csv",

        header=True,

        inferSchema=True

    )


    category = category.toDF(

        *[c.upper() for c in category.columns]

    )


    category = category.dropDuplicates(

        ["PRODUCTCATEGORYKEY"]

    )


    category = category.withColumn(

        "PRODUCT_CATEGORY_KEY",

        F.monotonically_increasing_id()

    )


    category = category.withColumnRenamed(

        "PRODUCTCATEGORYKEY",

        "PRODUCT_CATEGORY_ID"

    )


    category = category.withColumnRenamed(

        "CATEGORYNAME",

        "CATEGORY_NAME"

    )


    category = category.select(

        "PRODUCT_CATEGORY_KEY",
        "PRODUCT_CATEGORY_ID",
        "CATEGORY_NAME"

    )


    load_dataframe_to_snowflake(

        category.toPandas(),

        "CAPSTONE_GOLD_DB",

        "ANALYTICS",

        "DIMPRODUCTCATEGORY"

    )


    print("DIMPRODUCTCATEGORY loaded successfully.")

def load_product_subcategory_dimension():

    spark = get_spark_session()

    print("Loading DIMPRODUCTSUBCATEGORY...")


    subcategory = spark.read.csv(

        "datasets/lookups/Product Subcategories Lookup.csv",

        header=True,

        inferSchema=True

    )


    subcategory = subcategory.toDF(

        *[c.upper() for c in subcategory.columns]

    )


    subcategory = subcategory.dropDuplicates(

        ["PRODUCTSUBCATEGORYKEY"]

    )


    subcategory = subcategory.withColumn(

        "PRODUCT_SUBCATEGORY_KEY",

        F.monotonically_increasing_id()

    )


    subcategory = subcategory.withColumnRenamed(

        "PRODUCTSUBCATEGORYKEY",

        "PRODUCT_SUBCATEGORY_ID"

    )


    subcategory = subcategory.withColumnRenamed(

        "PRODUCTCATEGORYKEY",

        "PRODUCT_CATEGORY_ID"

    )


    subcategory = subcategory.withColumnRenamed(

        "SUBCATEGORYNAME",

        "SUBCATEGORY_NAME"

    )


    subcategory = subcategory.select(

        "PRODUCT_SUBCATEGORY_KEY",
        "PRODUCT_SUBCATEGORY_ID",
        "PRODUCT_CATEGORY_ID",
        "SUBCATEGORY_NAME"

    )


    load_dataframe_to_snowflake(

        subcategory.toPandas(),

        "CAPSTONE_GOLD_DB",

        "ANALYTICS",

        "DIMPRODUCTSUBCATEGORY"

    )


    print("DIMPRODUCTSUBCATEGORY loaded successfully.")

def load_territory_dimension():

    spark = get_spark_session()

    print("Loading DIMTERRITORY...")


    territory = spark.createDataFrame(

        read_table(

            database="CAPSTONE_SILVER_DB",

            schema="CURATED",

            table_name="SILVER_TERRITORY"

        )

    )


    territory = territory.toDF(

        *[c.upper() for c in territory.columns]

    )


    territory = territory.dropDuplicates(

        ["SALESTERRITORYKEY"]

    )


    territory = territory.withColumn(

        "TERRITORY_KEY",

        F.monotonically_increasing_id()

    )


    territory = territory.withColumnRenamed(

        "SALESTERRITORYKEY",

        "TERRITORY_ID"

    )


    territory = territory.select(

        "TERRITORY_KEY",

        "TERRITORY_ID",

        "REGION",

        "COUNTRY",

        "CONTINENT"

    )


    load_dataframe_to_snowflake(

        territory.toPandas(),

        "CAPSTONE_GOLD_DB",

        "ANALYTICS",

        "DIMTERRITORY"

    )


    print("DIMTERRITORY loaded successfully.")


if __name__ == "__main__":

    load_customer_dimension()

    load_product_dimension()

    load_product_category_dimension()

    load_product_subcategory_dimension()

    load_territory_dimension()
