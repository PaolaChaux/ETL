import time
import kafka
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from Main_Aqua_Quality.main_data.db_Dimesional_Modeling import run_query
from Main_Aqua_Quality.main_data.kafka import kafka_producer, kafka_consumer

def stream_data():
    sql = '''SELECT * 
    FROM Fact_WaterQuality
    '''
    water_df = run_query(sql)
    for index, row in water_df.iterrows():
        kafka_producer(row)
        time.sleep(1)

if __name__ == '__main__':
    # Crear el tópico si no existe
    from kafka.admin import AdminClient, NewTopic

    def create_topic(topic_name, num_partitions=1, replication_factor=1, bootstrap_servers='localhost:9092'):
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        topic_list = [NewTopic(topic_name, num_partitions, replication_factor)]
        admin_client.create_topics(topic_list)

    create_topic('kafka-water')

    # Iniciar el flujo de datos
    stream_data()

    # Iniciar el consumidor (esto puede ejecutarse en un hilo separado si se necesita)
    kafka_consumer()