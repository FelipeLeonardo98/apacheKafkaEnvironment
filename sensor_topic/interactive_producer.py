# Apache Kafka
from kafka import KafkaProducer
# Basic Python
import os, json, time
from dotenv import load_dotenv
from datetime import datetime
import logging

load_dotenv()
KAFKA_INTERNAL_PORT = os.getenv("KAFKA_INTERNAL_PORT")

# Retry until Kafka is ready
"""
for _ in range(10):
    try:
        print(f"TRYING TO CONNECT TO KAFKA_INTERNAL_PORT: {KAFKA_INTERNAL_PORT}")
        producer = KafkaProducer(
            #bootstrap_servers=f'kafka:{KAFKA_INTERNAL_PORT}',
            bootstrap_servers='localhost:29093',
            # NoBrokersAvailable
            #api_version=(3, 8, 0),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serializar a mensagem para JSON
            key_serializer=lambda k: k.encode('utf-8') # Serializar a mensagem para JSON
        )
        break
    except Exception as e:
        print(f"Kafka not ready yet, retrying... {e}")
        time.sleep(3)
else:
    raise Exception("Kafka broker not available")
"""


    
headers = [("metadata_example_one", b"xxx"), ("metadata_example_two", b"yyy")]

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

def interactive_generate_sensor_data(sensor_id: str, temperature: float):
    return {
        'sensor_id': sensor_id,
        'temperature': temperature,
        'timestamp': datetime.now().isoformat(),
        'location': sensors.get(sensor_id)
    }


if __name__ == '__main__':
    log_format = "[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
    try:
        print(f"TRYING TO CONNECT TO KAFKA_INTERNAL_PORT: {KAFKA_INTERNAL_PORT}")
        producer = KafkaProducer(
            #bootstrap_servers=f'kafka:{KAFKA_INTERNAL_PORT}',
            bootstrap_servers='localhost:29093',
            # NoBrokersAvailable and KafkaConnectionError: Unable to determine broker version.
            api_version=(3, 8, 0),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serializar a mensagem para JSON
            key_serializer=lambda k: k.encode('utf-8') # Serializar a mensagem para JSON
        )
    
    except Exception as e:
        print(f"Kafka not ready yet, retrying... {e}")

    topic = 'temperature_sensor_topic'
    time.sleep(1)
    while True:

        sensor_id = input("sensor_id: ")
        temperature = input("temperature: ")
        data = interactive_generate_sensor_data(sensor_id, temperature)
        logging.info(f"Sending: {data}")
        try:
            producer.send(topic, key=sensor_id, value=data, headers=headers)
            logging.info("Success")
        except Exception as e:
            logging.error(f"Error: {e}")
