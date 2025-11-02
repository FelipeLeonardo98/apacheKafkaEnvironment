# Python
import time
from dotenv import load_dotenv
import ast, os, logging
# kafka
from confluent_kafka import Consumer, KafkaException

# Kafka config
load_dotenv()
IS_LOCAL = True
if IS_LOCAL:
    KAFKA_BOOTSTRAP_ADDRESS = "localhost:29093"
TOPIC_NAME = "transactions_topic"

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_ADDRESS,
    'group.id': 'group-transactions',
    'auto.offset.reset': 'latest',
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC_NAME])

# Logging
log_format = "[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")


try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        print(msg)
except KeyboardInterrupt:
    logging.info("Shutting down consumer...")

finally:
    consumer.close()