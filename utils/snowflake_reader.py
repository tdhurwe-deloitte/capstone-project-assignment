import pandas as pd

from config.snowflake_config import get_connection



def read_table(
        database,
        schema,
        table_name
):

    conn = get_connection()

    cursor = conn.cursor()

    try:

        query = f"""
        SELECT *
        FROM {database}.{schema}.{table_name}
        """

        print("\nExecuting:")
        print(query)


        cursor.execute(query)


        rows = cursor.fetchall()


        columns = [
            column[0]
            for column in cursor.description
        ]


        df = pd.DataFrame(
            rows,
            columns=columns
        )


        df.columns = [
            col.upper()
            for col in df.columns
        ]


        return df


    finally:

        cursor.close()

        conn.close()