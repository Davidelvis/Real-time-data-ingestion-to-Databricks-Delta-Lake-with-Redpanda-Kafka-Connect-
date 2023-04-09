import os
from dotenv import load_dotenv
from delta import *
import pyspark as pyspark

def get_spark_session():
    load_dotenv()
    broker_ip = os.environ.get('REDPANDA_BROKER_IP')
    topic = os.environ.get('REDPANDA_TOPIC')
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark