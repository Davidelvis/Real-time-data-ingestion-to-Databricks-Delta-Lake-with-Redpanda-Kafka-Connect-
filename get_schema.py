import os
import pyspark as pyspark
import json
from dotenv import load_dotenv
from delta import *
from IPython.display import display
from pyspark.sql.functions import expr
from pyspark.sql.functions import from_json, col
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import *


def get_schema(spark):
    load_dotenv()
    broker_ip = os.environ.get('REDPANDA_BROKER_IP')
    topic = os.environ.get('REDPANDA_TOPIC')
    df_json = (spark.read
                .format("kafka")
                .option("kafka.bootstrap.servers", broker_ip)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
                # filter out empty values
                .withColumn("value", expr("string(value)"))
                .filter(col("value").isNotNull())
                # get latest version of each record
                .select("key", expr("struct(offset, value) r"))
                .groupBy("key").agg(expr("max(r) r")) 
                .select("r.value"))

    # decode the json values
    df_read = spark.read.json(df_json.rdd.map(lambda x: x.value), multiLine=True)
    return df_read.schema.json()

def get_table_df(spark):
    load_dotenv()
    broker_ip = os.environ.get('REDPANDA_BROKER_IP')
    topic = os.environ.get('REDPANDA_TOPIC')
    schema = get_schema(spark)
    table_df = ( 
                spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", broker_ip)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("mergeSchema", "true")
                .option("includeHeaders", "true")
                .option("failOnDataLoss", "false")
                .load()
                .withColumn("value", expr("string(value)"))
                .filter(col("value").isNotNull())
                .withColumn('value', from_json(col("value"), schema))
                .select('value.*')
    )
    return table_df