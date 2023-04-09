# Real-time data ingestion to Databricks Delta Lake with Redpanda (Kafka Connect)


## Introduction

We are in the era where Real-time data ingestion has become one of the critical requirement for many organizations which are seeking to tap the value from their data in Real-time and near Real-time. Real-time data ingestion has grown to become one of the brands that sets apart many organizations in the competing market, but a research from Databricks revealed that, a staggering 73% of a company's data goes unused for analytics and decision-making when stored in a data lake. This means that machine learning models are doomed to return inaccurate results, perform poorly in real-world scenarios and many other implications.

Delta lake is a game changer for big data, developed as an advanced open source storage layer, it provides an abstract layer on top of existing data lakes and it is optimized for Parquet-based storage. Databricks Delta lake  provides features such as:

* Support for ACID transactions - ACID stands for Atomicity, Consistency, Isolation, and Durability. All the transactions made on the delta lakes are ACID compliant.
* Schema enforcement - When writing data to storage, Delta lakes will enforce schema which helps in maintaining the columns and data types and achieving data reliability and high quality data.
* Scalable metadata handling - Delta lake will scale out all metadata processing operations using compute engines like Apache Spark and Apache Hive, allowing it to process the metadata for petabytes of data efficiently.

Databricks Delta lake integration can be used for a variety use cases to store, manage and analyze volumes of incoming data. For example, In customer analytics, Delta lakes can be used to store and process customer data for analytics purposes, and also it can be used to process IoT data because of it is ability to handle large volumes of data and even performing real-time data processing.

In this tutorial, you will learn how to do the following:

* Create and configure Databricks Delta lakes
* Set up and run a Redpanda cluster and create topics for Kafka Connect usage
* Configure and run a Kafka Connect cluster for Redpanda and Databricks Delta lake integration

## Deep dive into Databricks deployment architecture

Databricks deployment is structured to provide a fully managed, scalable, reliable and secure platform for data processing and analytics tasks. It's architecture is split into two main components: the **control plane** and **data plane** and this enables secure cross-functional team collaboration while keeping a significant amount of backend services managed by Databricks.

* The control plane - It includes the backend services that Databricks manages within the could environment including access control, user authetication and resource management.
* The data plane - It's used to manage the storage and processing of data. It includes resources such as Databricks Cluster, which is a set of virtual machines that are provisioned on-demand to run notebooks and jobs, and the Databricks Delta Lake, which is a storage layer that provide an abstract layer on top of your data lake in the cloud.

The control plane and data plane work together to provide a smooth experience for the data teams.
The separation of the control plane and data plane makes it possible for Databricks to scale each component independently which ensures the deployment architecture remains scalable, reliable and performant even as workloads grow in size and complexity.

The overall Redpanda BYOC (bring your own cloud) deployment architecture follows the same structure as that of Databricks deployment architecture in the following ways:

* Both Redpanda and Databricks architectures leverage the underlying cloud infrastructure to provide reliability, security and scalability for data processing and analytics tasks.
* Just like Databricks, Redpanda BYOC contains the control plane which is responsible for managing Redpanda clusters including provisioning and scaling, also the data plane which is responsible for processing and storing data.

Redpanda BYOC and Databricks both provide an interactive and collaborative workspace for data teams to operate in, facilitating faster iterations and the delivery of value.

## Prerequisites

You'll need the following for this tutorial:

* Docker installed on your machine, preferably Docker Desktop if you’re on Windows/Mac (this article uses Docker 20.10.21)
* A machine to install Redpanda and Kafka Connect (this tutorial uses Linux, but you can run Redpanda on Docker, MacOS and Windows, as well)
* Python 3.10 or higher.
* Java version 8 or later( this tutorial uses java 11)
* Delta Lake 1.2.1

## Use case: Implementing data ingestion to Databricks Delta lakes with Redpanda (Kafka Connect)

In this part we create a fictitious scenario to help you understand how you can ingest data to Databricks Delta Lake using Kafka Connect with Redpanda. This is just for demonstration purposes only.

Let us imagine that you work as a Lead engineer at a e-commerce company known as EasyShop,that sells products its website. Your company is experiencing a rapid growth and due to this it is expanding its operations to new markets.

To better understand customer behavior and preferences, the company wants to create a unified view of its sales and customer data which includes user clicks, page views, and orders.
To achieve this, you decide to set up a data pipeline that ingests data in Real-time from the company website into Databricks Delta Lake.

The following diagram explains the high-level architecture:

![High level architecture](https://imgur.com/a/4GAEPjV)

## Setting up Redpanda
In this article, we assume that you already have Redpanda installed via Docker. If you have note installed, please refer to this [Documentation](https://docs.redpanda.com/docs/get-started/quick-start/) on how to installed Redpanda via Docker.

To check if these Redpanda is up and running, execute the command `docker ps`. You’ll see an output like these if the service is running:
```Bash
CONTAINER ID   IMAGE                 COMMAND                  CREATED          STATUS          PORTS                                                                     NAMES
bb2305256285   vectorized/redpanda   "/entrypoint.sh redp…"   16 minutes ago   Up 16 minutes   8081/tcp, 9092/tcp, 9644/tcp, 0.0.0.0:8082->8082/tcp, :::8082->8082/tcp   redpanda-1
```

You can show Redpanda is running via rpk CLI on the docker by executing this command `docker exec -it redpanda-1 rpk cluster info` where `redpanda-1` is the container_name.
The output of the command should look similar to the following:
```Bash
CLUSTER
=======
redpanda.36dda01b-4e8a-4949-bb38-04f83a13b009

BROKERS
=======
ID    HOST     PORT
0*    0.0.0.0  9092
```

## Creating a Redpanda Topic

To create a topic in Redpanda, you can use rpk which is a CLI tool for connecting to and interacting with Redpanda brokers. Open a terminal and connect to Redpanda’s container session by executing the command `docker exec -it redpanda-1 bash`. You should see your terminal session connected now to the `redpanda-1` container:
```Bash
redpanda@bb2305256285:/$ 
```

Next, execute the command `rpk topic create customer-data` to create the topic customer-data. You can check the existence of the topic created with the command `rpk topic list`. This will list all the topics which are up and running and you should see an output like this:
```Bash
NAME           PARTITIONS  REPLICAS
customer-data  1           1
```
OR you can verify the created topics by getting the cluster information using the command `rpk cluster info`.

You will see the following output:
```Bash
CLUSTER
=======
redpanda.36dda01b-4e8a-4949-bb38-04f83a13b009

BROKERS
=======
ID    HOST     PORT
0*    0.0.0.0  9092

TOPICS
======
NAME           PARTITIONS  REPLICAS
customer-data  1           1
```

## Developing the producer code for a scenario

You have Redpanda services running and a topic available in the redpanda container to receive events, move on to create a application that will publish messages to this topic.
This will be a two step process:

* First, you will set up a virtual environment and install the necessary dependencies.
* Create a producer code to generate sample customer data and publish it to the created customer-data topic

## Setting up virtual environment

Before creating a producer and consumer code, you have to your Python virtual environment and install the dependencies.

To begin, create a project directory,`Real-time data ingestion to Databricks Delta Lake with Redpanda (Kafka Connect)` in your machine.

Create subdirectories `customer-data` and `delta-lake` in the project directory.
The `customer-data` directory will hold the Python application that will publish our data, and the `delta-lake` directory will store the data.

While inside the `customer-data` subdirectory, run `python3 -m venv venv` command to create a virtual environment setup for this demo project.

In the same subdirectory, create a  `requirements.txt` file and paste the following content:

```Text
kafka-python==2.0.2
```

To install the dependencies, run `pip install -r requirements.txt` .

## Producing data

Create a file called `redpanda_producer.py` that has producer code that generates and inserts 10000 random JSON entries into the `customer-data` topic.

Paste this piece of code into the file:
```Python
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from time import sleep
from faker import Faker
import faker_commerce
from faker.providers import internet
import json
import os
from dotenv import load_dotenv


def main():
    topic_name = os.environ.get('REDPANDA_TOPIC')
    broker_ip = os.environ.get('REDPANDA_BROKER_IP')
    types_of_categories = ['clothing', 'electronics', 'home & kitchen', 'beauty & personal care', 'toys & games']
    fake = Faker()
    fake.add_provider(faker_commerce.Provider)
    fake.add_provider(internet)
    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin = KafkaAdminClient(bootstrap_servers=broker_ip)
        admin.create_topics([topic])
    except Exception:
        print(f"Topic {topic_name} is already created")

    producer = KafkaProducer(bootstrap_servers=[broker_ip],
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))
    for i in range(10000):
        fake_date = fake.date_this_month(True)
        product_id = fake.pyint(1, 10000)
        categories = fake.word(ext_word_list=types_of_categories)
        product_name = fake.product_name()
        name = fake.name()
        email_addr = fake.email(name)
        str_date = str(fake_date)
        units_sold = fake.pyint(1, 25)
        unit_price = fake.pyint(10, 500)
        country = fake.country()
        producer.send(topic_name, {'date': str_date, 'product_name': product_name, 'category': categories,
                                   'name': name, 'email': email_addr, 'units_sold': units_sold,
                                   'unit_price': unit_price, 'country': country})
        print("Inserted entry ", i, " to topic", topic_name)
        sleep(1)


load_dotenv()
main()
```
To run the script, execute the command `python3 redpanda_producer.py` and if it runs successfully, you should see the messages produced on the topics.

You can also check the output using Redpanda’s CLI tool, `rpk` by running the following command:

`rpk topic consume customer-data --brokers <IP:PORT>`

You should see the output below, which has the `topic`, `value`, `timestamp`,`partition`, and `offset` fields.

## Configuring Databricks Delta lake

In this section, you'll configure Delta Lake using Spark session. For this, we will use the `configure_delta-lake.py` script from our Github Demo repo.

Running the `configure_delta-lake` script will accomplish the following:

1. Generate a Spark session
2. Obtain the schema from the topic we are generating the data
3. Create the Delta table

Let us look deep into the steps

Using Pyspark, you'll create a new Spark session with the correct packages and add the corresponding additional Delta Lake dependencies to interact with Delta Lake. You can do this using the following code:

```Python
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
```

You can find the packages used in the `PYSPARK_ARGS` variable inside the `.env` file.

```Bash
REDPANDA_BROKER_IP=0.0.0.0:9092
REDPANDA_TOPIC=customer-data
PYSPARK_SUBMIT_ARGS='--packages io.delta:delta-core_2.12:1.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'
DELTA_LAKE_TABLE_DIR='/tmp/delta/customer-table'
DELTA_LAKE_CHECKOUT_DIR='/tmp/checkpoint/'
```

Once, you've set up the Spark session, you'll use it to obtain the schema of the `customer-data` topic.
You can find this logic in the `get_schema()` function inside `get_schema.py` file.

```Python
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
```

You have Spark session running and the schema from the topic, now you can go ahead to create the Delta table. The paths are configurable and can be modified as per your preference by editing the `.env`file.
You can use the following code to create a delta table:

```Python
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

```
You've successfully  configured Databricks Delta lake and created a Delta table for the data to be ingested.
## Setting up Kafka Connect

Kafka Connect is an integration tool released with the Apache KafkaⓇ project. It’s scalable and flexible, and it provides reliable data streaming between Apache Kafka and external systems. You can use it to integrate with any system, including databases, search indexes, and cloud storage providers. Redpanda is fully compatible with the Kafka API.

Kafka Connect uses source and sink connectors for integration. Source connectors stream data from an external system to Kafka, while sink connectors stream from Kafka to an external system.

To install Kafka connect, you have to download the Apache Kafka package. Navigate to the [Apache downloads page](https://www.apache.org/dyn/closer.cgi?path=/kafka/3.1.0/kafka_2.13-3.1.0.tgz) for Kafka and click the suggested download link for the Kafka 3.1.0 binary package.

Run the following commands to create a folder called `pandabooks_integration` in your home directory and extract the Kafka binaries file to this directory. 

```Bash
mkdir pandabooks_integration && \
mv ~/Downloads/kafka_2.13-3.1.0.tgz pandabooks_integration && \
cd pandabooks_integration && \
tar xzvf kafka_2.13-3.1.0.tgz
```

## Configuring the connect cluster

Before running a kafka connect cluster, you have to set up a configuration file in the properties format.
Navigate to the `pandabooks_integration` and create a folder called `configuration`. While inside the folder, create a file known as `connect.properties` and paste this contents:

```Python
#Kafka broker addresses
bootstrap.servers=

#Cluster level converters
#These applies when the connectors don't define any converter
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter

#JSON schemas enabled to false in cluster level
key.converter.schemas.enable=true
value.converter.schemas.enable=true

#Where to keep the Connect topic offset configurations
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000

#Plugin path to put the connector binaries
plugin.path=
```

Set the `bootstrap.servers` value to `localhost:9092` to configure the Connect cluster to use the Redpanda cluster.

Also it is necessary to configure `plugin.path`, which you’ll use to put the connector binaries in.

Create a folder called `plugins` in the `pandabooks_integration` directory. Navigate to the `plugins` folder and create another folder and call it `delta-lake`, this is where you will add the connector binaries.

Navigate to this [web page](https://docs.confluent.io/kafka-connectors/databricks-delta-lake-sink/current/overview.html#databricks-delta-lake-sink-connector-cp) and click **Download** to download the archived binaries. Unzip the file and copy the files in the **lib** folder into a folder called kafka-connect-delta-lake, placed in the `plugins` directory.


The final folder structure for pandapost_integration should look like this:

```bash
pandapost_integration
├── configuration
│   ├── connect.properties
├── plugins
│   ├── kafka-connect-delta-lake
│   │   ├── aggs-matrix-stats-client-7.9.3.jar
│   │   ├── ...
│   │   └── snakeyaml-1.27.jar
└── kafka_2.13-3.1.0
```

You will need to change the `plugin.path` value to `/home/_YOUR_USER_NAME_/pandabooks_integration/plugins`. This will configure the Connect cluster to use the Redpanda cluster.

Now, the final `connect.properties` file should look like this:

```Python
#Kafka broker addresses
bootstrap.servers=localhost:9092

#Cluster level converters
#These applies when the connectors don't define any converter
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter

#JSON schemas enabled to false in cluster level
key.converter.schemas.enable=true
value.converter.schemas.enable=true

#Where to keep the Connect topic offset configurations
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000

#Plugin path to put the connector binaries
plugin.path=_YOUR_HOME_DIRECTORY_/pandapost_integration/plugins
```

## Configuring the Delta Lake Connector

You have successfully set up connector plugins in a kafka connector cluster to achieve integration with external systems but this is not enough, you need to configure the sink connector, that is Delta Lake Connector plugin that will enable Kafka connect to write data directly to Delta Lake.

To do this, create a file named `delta-lake-sink-connector.properties` in the `~/pandabooks_integration/configuration` directory and paste the following contents:

```Python
name=delta-lake-sink-connector

# Connector class
connector.class= io.delta.connect.kafka.DeltaLakeSinkConnector

# Format class
format.class=io.delta.standalone.kafka.DeltaInputFormat

# The key converter for this connector
key.converter=org.apache.kafka.connect.storage.StringConverter

# The value converter for this connector
value.converter=org.apache.kafka.connect.json.JsonConverter

# Identify, if value contains a schema.
# Required value converter is `org.apache.kafka.connect.json.JsonConverter`.
value.converter.schemas.enable=false

tasks.max=1

# Topic name to get data from
topics= customer-data

# Table to ingest data into
tableName = customer-table

key.ignore=true

schema.ignore=true

```

Remember to change the following values for the keys in the elasticsearch-sink-connector.properties file:

1. connector.class
2. topic

## Running the Kafka Connect cluster

You have to run the Kafka connect cluster with the configurations that you have made. Open a new terminal, navigate to the `_YOUR_HOME_DIRECTORY_/pandapost_integration/configuration` directory and run the following command:

```Bash
../kafka_2.13-3.1.0/bin/connect-standalone.sh connect.properties delta-lake-sink-connector.properties
```
If everything was done correctly, the output will look like this:

```Bash
...output omitted...
 groupId=connect-elasticsearch-sink-connector] Successfully joined group with generation Generation{generationId=25, memberId='connector-consumer-elasticsearch-sink-connector-0-eb21795e-f3b3-4312-8ce9-46164a2cdb27', protocol='range'} (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator:595)
[2022-04-17 03:37:06,872] INFO [elasticsearch-sink-connector|task-0] [Consumer clientId=connector-consumer-elasticsearch-sink-connector-0, groupId=connect-elasticsearch-sink-connector] Finished assignment for group at generation 25: {connector-consumer-elasticsearch-sink-connector-0-eb21795e-f3b3-4312-8ce9-46164a2cdb27=Assignment(partitions=[news-reports-0])} (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator:652)
[2022-04-17 03:37:06,891] INFO [elasticsearch-sink-connector|task-0] [Consumer clientId=connector-consumer-elasticsearch-sink-connector-0, groupId=connect-elasticsearch-sink-connector] Successfully synced group in generation Generation{generationId=25, memberId='connector-consumer-elasticsearch-sink-connector-0-eb21795e-f3b3-4312-8ce9-46164a2cdb27', protocol='range'} (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator:761)
[2022-04-17 03:37:06,891] INFO [elasticsearch-sink-connector|task-0] [Consumer clientId=connector-consumer-elasticsearch-sink-connector-0, groupId=connect-elasticsearch-sink-connector] Notifying assignor about the new Assignment(partitions=[news-reports-0]) (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator:279)
[2022-04-17 03:37:06,893] INFO [elasticsearch-sink-connector|task-0] [Consumer clientId=connector-consumer-elasticsearch-sink-connector-0, groupId=connect-elasticsearch-sink-connector] Adding newly assigned partitions: news-reports-0 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator:291)
[2022-04-17 03:37:06,903] INFO [elasticsearch-sink-connector|task-0] [Consumer clientId=connector-consumer-elasticsearch-sink-connector-0, groupId=connect-elasticsearch-sink-connector] Setting offset for partition news-reports-0 to the committed offset FetchPosition{offset=3250, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 0 rack: null)], epoch=absent}} (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator:844)
```

## Conclusion

In conclusion, real-time data ingestion to Databricks Delta Lake with Redpanda (Kafka Connect) is a powerful and flexible solution for processing and analyzing streaming data. In this article, we've discussed step by step on implementing data ingestion to Databricks Delta Lake with Redpanda (Kafka Connect).

Remember, you can find the code resources for this tutorial in this repository.

Interact with Redpanda’s developers directly in the [Redpanda Community on Slack](https://redpanda.com/slack), or contribute to Redpanda’s [source-available GitHub repo here](https://github.com/redpanda-data/redpanda/). To learn more about everything you can do with Redpanda, check out [our documentation here](https://docs.redpanda.com/docs/home).