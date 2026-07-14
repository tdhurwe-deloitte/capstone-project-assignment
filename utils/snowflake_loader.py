from snowflake.connector.pandas_tools import write_pandas # type: ignore

from config.snowflake_config import get_connection



def load_dataframe_to_snowflake(
        pandas_df,
        database,
        schema,
        table_name
):
    """
    Loads a Pandas DataFrame into a Snowflake table.

    Parameters:
        pandas_df : Pandas DataFrame
        database  : Snowflake database name
        schema    : Snowflake schema name
        table_name: Target table name
    """

    conn = get_connection()

    try:

        # Switch database and schema
        cursor = conn.cursor()

        cursor.execute(
            f"USE DATABASE {database}"
        )

        cursor.execute(
            f"USE SCHEMA {schema}"
        )


        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=pandas_df,
            table_name=table_name,
            auto_create_table=False,
            overwrite=False
        )


        if success:
            print(
                f"Successfully loaded {nrows} rows into "
                f"{database}.{schema}.{table_name}"
            )

        else:
            raise Exception(
                f"Failed loading {table_name}"
            )


    finally:

        conn.close()