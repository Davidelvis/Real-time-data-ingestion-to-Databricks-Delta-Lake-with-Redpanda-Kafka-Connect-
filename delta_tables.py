import os
import pyspark as pyspark
import json
from dotenv import load_dotenv
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *


def create_delta_tables(spark, table_df):
    load_dotenv()
    table_path = os.environ.get('DELTA_LAKE_TABLE_DIR')
    table = (spark
        .createDataFrame([], table_df.schema)
        .write
        .option("mergeSchema", "true")
        .format("delta")
        .mode("append")
        .save(table_path))

