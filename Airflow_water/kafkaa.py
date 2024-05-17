import pandas as pd
import logging
from datetime import datetime
from json import dumps, loads
from kafka import KafkaConsumer, KafkaProducer, AdminClient, NewTopic


def kafka_producer(row):
    producer = KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=['localhost:9092'],
    )

    message = row.to_dict()
    producer.send('kafka-water', value=message)
    logging.info("Enviado")


def kafka_consumer():
    consumer = KafkaConsumer(
        'kafka-water',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=['localhost:9092']
    )

    for message in consumer:
        df = pd.json_normalize(data=message.value)
        
        
# def create_topic(topic_name, num_partitions=1, replication_factor=1, bootstrap_servers='localhost:9092'):
#     admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
#     topic_list = [NewTopic(topic_name, num_partitions, replication_factor)]
#     admin_client.create_topics(topic_list)

