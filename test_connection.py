from utils.snowflake_reader import read_table


df = read_table(
    database="CAPSTONE_BRONZE_DB",
    schema="RAW",
    table_name="BRONZE_SALES"
)


print(df.head())
print(df.dtypes)