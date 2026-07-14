import pandas as pd

from config.snowflake_config import get_connection


def read_table(database, schema, table_name, columns="*"):

    conn = get_connection()

    try:

        query = f"""
        SELECT {columns}
        FROM {database}.{schema}.{table_name}
        """

        print("Executing:")
        print(query)


        df = pd.read_sql(
            query,
            conn
        )

        return df


    finally:

        conn.close()