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