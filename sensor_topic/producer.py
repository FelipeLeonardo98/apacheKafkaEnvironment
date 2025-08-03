# Apache Kafka
from kafka import KafkaProducer
import logging
# Basic Python
from dotenv import load_dotenv
from faker import Faker
import os, json, time, random
from datetime import datetime

load_dotenv()
KAFKA_INTERNAL_PORT = os.getenv("KAFKA_INTERNAL_PORT")

# Initialize Faker and KafkaProducer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=f'kafka:{KAFKA_INTERNAL_PORT}',
    api_version=(3, 8, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serializar a mensagem para JSON
    key_serializer=lambda k: k.encode('utf-8') # Serializar a mensagem para JSON
)

headers = [
    ("metadata_example_one",b"xxx"),
    ("metadata_example_two",b"yyy")
]

sensors = {
    "1": "location_3",
    "2": "location_1",
    "3": "location_4",
    "4": "location_2",
    "5": "location_1",
    "6": "location_4",
    "7": "location_2",
    "8": "location_3",
    "9": "location_1",
    "10": "location_4",
    "11": "location_2",
    "12": "location_3",
    "13": "location_4",
    "14": "location_1",
    "15": "location_2",
    "16": "location_3",
    "17": "location_1",
    "18": "location_2",
    "19": "location_3",
    "20": "location_4"
}

def generate_temperature_data(dict_sensors: dict = sensors):
    sensor_id = str(random.randint(1, 20))
    location_value = dict_sensors.get(sensor_id)
    return {
        'sensor_id': sensor_id,
        'temperature': round(random.uniform(-10.0, 40.0), 2),  # Random temperature between -10°C and 40°C
        'timestamp': datetime.now().isoformat(),  # Timestamp of the reading
        'location': location_value
    }


if __name__ == '__main__':
    log_format = "[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")

    topic = 'temperature_sensor_topic'
    
    while True:
        for i in range(1000):
            data = generate_temperature_data()
            key = data['sensor_id']  # Use the 'sensor_id' as the key
            logging.info(f"Sending the follow record to Kafka Cluster: {data}")
            producer.send(topic, key=key, value=data, headers=headers)  # Send both key and value
            time.sleep(3)  # Send a message every tshree