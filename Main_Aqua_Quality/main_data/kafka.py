from datetime import datetime
from json import dumps, loads
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
import logging

def kafka_producer(row):
    producer = KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=['localhost:9092'],
    )

    message = row.to_dict()
    producer.send('kafka-water', value=message)
    logging.info("Message sent")


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