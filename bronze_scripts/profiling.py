from pyspark.sql.functions import col, count, when

from config.spark_config import get_spark_session


DATA_PATH = "datasets"


FILES = {
    "Sales_2020": "datasets/sales/Sales Data 2020.csv",
    "Sales_2021": "datasets/sales/Sales Data 2021.csv",
    "Sales_2022": "datasets/sales/Sales Data 2022.csv",

    "Returns": "datasets/returns/Returns Data.csv",

    "Customer": "datasets/lookups/Customer Lookup.csv",

    "Product": "datasets/lookups/Product Lookup.csv",

    "Product_Category":
        "datasets/lookups/Product Categories Lookup.csv",

    "Product_Subcategory":
        "datasets/lookups/Product Subcategories Lookup.csv",

    "Territory":
        "datasets/lookups/Territory Lookup.csv",

    "Calendar":
        "datasets/lookups/Calendar Lookup.csv"
}


def profile_dataframe(df, name):

    print("\n==============================")
    print(name)
    print("==============================")


    print("\nRow Count:")
    print(df.count())


    print("\nColumns:")
    print(df.columns)


    print("\nSchema:")
    df.printSchema()


    print("\nNull Values:")

    null_count = (
        df.select(
            [
                count(
                    when(
                        col(c).isNull(),
                        c
                    )
                ).alias(c)
                for c in df.columns
            ]
        )
    )

    null_count.show()


    print("\nDuplicate Rows:")

    duplicates = (
        df.count()
        -
        df.dropDuplicates().count()
    )

    print(duplicates)



def main():

    spark = get_spark_session()


    for name,path in FILES.items():

        df = (
            spark.read
            .option("header","true")
            .option("inferSchema","true")
            .csv(path)
        )


        profile_dataframe(df,name)


    spark.stop()



if __name__ == "__main__":
    main()