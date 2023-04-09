import os
from dotenv import load_dotenv
from delta import *
from time import sleep
from setup_spark import get_spark_session
from get_schema import get_table_df
from delta_tables import create_delta_tables
import pyspark as pyspark


spark = get_spark_session()
table_df = get_table_df(spark)
create_delta_tables(spark, table_df)
print('*********************DELTA TABLE CREATED SUCCESSFULLY****************************')