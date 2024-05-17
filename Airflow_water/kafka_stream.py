import time
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from Main_Aqua_Quality.main_data.db_Dimesional_Modeling import run_query
from Main_Aqua_Quality.main_data.kafka import kafka_producer

def create_topic(topic_name, num_partitions=1, replication_factor=1, bootstrap_servers='localhost:9092'):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print(f'Topic {topic_name} created successfully')

def kafka_producer(row):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: dumps(v).encode('utf-8')
    )
    message = row.to_dict()
    producer.send('kafka-water', value=message)
    producer.flush()
    print("Message sent")

def kafka_consumer():
    consumer = KafkaConsumer(
        'kafka-water',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    for message in consumer:
        df = pd.json_normalize(message.value)
        print(df)

def stream_data():
    sql = '''SELECT * 
    FROM Fact_WaterQuality
    '''
    water_df = run_query(sql)
    for index, row in water_df.iterrows():
        kafka_producer(row)
        time.sleep(1)

if __name__ == '__main__':
    create_topic('kafka-water')
    stream_data()


    kafka_consumer()
