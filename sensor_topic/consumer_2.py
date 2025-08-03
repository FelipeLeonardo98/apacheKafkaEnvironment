# Apache Kafka
from kafka import KafkaConsumer
# Basic Python
from dotenv import load_dotenv
from time import time
from datetime import datetime
import os, json, time, uuid, json, logging

load_dotenv()
KAFKA_INTERNAL_PORT = os.getenv("KAFKA_INTERNAL_PORT")

# Initialize KafkaConsumer
consumer = KafkaConsumer(
    'temperature_sensor_topic',  # The Kafka topic to consume from
    api_version=(3, 8, 0),
    bootstrap_servers=f'kafka:{KAFKA_INTERNAL_PORT}',
    auto_offset_reset='earliest',  # Start reading at the beginning of the topic if no offset is found
    enable_auto_commit=False,
    group_id=f'temperature_sensor_consumer_group_{str(uuid.uuid4())}',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# GLOBALS
MAX_BATCH_RECORDS = 5
TIMEMOUT_MS = 30000
queue = []
MAX_BATCH_SIZE = 5
TIME_BASE = time.time()
TIMEOUT = 30 # IN SECONDS

def populate_queue(record:str, max_batch_size=MAX_BATCH_SIZE):
    queue.append(record)
    if len(queue) >= max_batch_size:
        return queue
    else:
        return None
    
if __name__ == '__main__':
    log_format = "[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")


    while True:
        logging.info("---------------- FETCH NEW POLL -------------------------------")
        records = consumer.poll(timeout_ms=1000, max_records=MAX_BATCH_SIZE)
       
        for topic_partition, messages in records.items():
            for message in messages:
                result = populate_queue(record=str(message.value))
                #logging.info(f"Received message from partition {topic_partition.partition}: {message.value}")

        if len(queue) >= MAX_BATCH_RECORDS or (time.time() - TIME_BASE) > TIMEOUT:
            if queue:
                logging.info(f"Processing batch of len {len(queue)}")
                for record in queue:
                    logging.info(record)
                consumer.commit()
                queue.clear()
            TIME_BASE = time.time()
