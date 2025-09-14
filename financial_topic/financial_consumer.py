# Confluent Kafka
from confluent_kafka import Consumer, KafkaException
# Basic Python
from dotenv import load_dotenv
import json, logging, os

load_dotenv()
KAFKA_INTERNAL_PORT = os.getenv("KAFKA_INTERNAL_PORT")
IS_LOCAL = True
if IS_LOCAL:
    KAFKA_BOOTSTRAP_ADDRESS = "localhost:29093"
TOPIC_NAME = "test_transactions_topic"

# Configuração do consumidor
conf = {
    'bootstrap.servers': f"{KAFKA_BOOTSTRAP_ADDRESS}",
    'group.id': 'group',
    'auto.offset.reset': 'latest',  # Inicia leitura do último offset
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC_NAME])

if __name__ == '__main__':
    log_format = "[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")

    try:
        while True:
            msg = consumer.poll(timeout=10.0)  # Espera 1 segundo por mensagem

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            # Decodifica a mensagem JSON
            #message_value = json.loads(msg.value().decode('utf-8'))
            raw_value = msg.value()
            logging.info(f"Raw message: {raw_value}")

    except KeyboardInterrupt:
        logging.info("Shutting down consumer...")

    finally:
        consumer.close()
