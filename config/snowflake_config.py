import os

import snowflake.connector # type:ignore
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """
    Creates and returns a Snowflake connection.
    """

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )