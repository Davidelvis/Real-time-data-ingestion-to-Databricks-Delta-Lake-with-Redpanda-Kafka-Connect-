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


def load_table_data(spark):
    load_dotenv()
    table_path = os.environ.get('DELTA_LAKE_TABLE_DIR')
    df_delta = (spark.read
            .format("delta")
            .load(table_path))

    return df_delta

def delete_entry(spark, name):
    load_dotenv()
    table_path = os.environ.get('DELTA_LAKE_TABLE_DIR')
    deltaTable = DeltaTable.forPath(spark, table_path)
    deltaTable.delete(col("name") == name)

def get_full_history(spark):
    load_dotenv()
    table_path = os.environ.get('DELTA_LAKE_TABLE_DIR')
    deltaTable = DeltaTable.forPath(spark, table_path)
    historyDF = deltaTable.history()
    historyDF.show()

def restore_to_version(spark, version_number):
    load_dotenv()
    table_path = os.environ.get('DELTA_LAKE_TABLE_DIR')
    df = spark.read.format("delta").option("versionAsOf", version_number).load(table_path)
    df.write.format("delta").mode("overwrite").save(table_path)
    output_df = load_table_data(spark)
    output_df.show()