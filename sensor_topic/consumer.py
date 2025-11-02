# Apache Kafka
from kafka import KafkaConsumer
# Basic Python
from dotenv import load_dotenv
import json, logging, os

load_dotenv()
KAFKA_INTERNAL_PORT = os.getenv("KAFKA_INTERNAL_PORT")

# Initialize KafkaConsumer
consumer = KafkaConsumer(
    'temperature_sensor_topic', # The Kafka topic to consume from
    api_version=(3, 8, 0),
    bootstrap_servers=f'localhost:29093',
    auto_offset_reset='latest',  # Start reading at the beginning of the topic if no offset is found
    enable_auto_commit=True,
    group_id='temperature_sensor_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

if __name__ == '__main__':
    log_format = "[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")

    for message in consumer:
    
        logging.info("#####################")
        logging.info(f"Metadata information\n Partition: {message.partition} - Headers: {message.headers}")
        logging.info("#####################")

        logging.info(f"Received: {message.value}")
        