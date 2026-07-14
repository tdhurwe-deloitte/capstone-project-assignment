from pyspark.sql import functions as F

from config.spark_config import get_spark_session

from utils.snowflake_reader import read_table

from utils.snowflake_loader import load_dataframe_to_snowflake
from pyspark.sql.window import Window