# Apache Kafka
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
# Basic Python
import os, time, json
from dotenv import load_dotenv
import logging
# Internals
from services.financial_data import generate_transaction_bank

# Globals
log_format = "[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
load_dotenv()

# Servers ports and Kafka Settings
KAFKA_INTERNAL_PORT = os.getenv("KAFKA_INTERNAL_PORT")
IS_LOCAL = True
if IS_LOCAL:
    KAFKA_BOOTSTAP_ADDRESS = "localhost:29093"
else:
    KAFKA_BOOTSTAP_ADDRESS = f'kafka:{KAFKA_INTERNAL_PORT}'

TOPIC_NAME =  "test_transactions_topic"
print("PORTS: KAFKA_INTERNAL_PORT: ", KAFKA_INTERNAL_PORT)


try:
    # Necessary for serializing the key
    string_serializer = StringSerializer("utf_8")

    # Producer settings
    producer_conf = {
        "bootstrap.servers": f'{KAFKA_BOOTSTAP_ADDRESS}',
      #  "key.serializer": string_serializer  # opcional, se quiser chave string
    }

    producer = SerializingProducer(producer_conf)

    def delivery_report(err, msg):
        """ Delivery Callback """
        if err is not None:
            logging.error(f"Error when trying to send the message: {err}")
        else:
            logging.info(f"Mesagen sent to: {msg.topic()}, partition: [{msg.partition()}], offset: {msg.offset()}")
        
except Exception as e:
    logging.error(f"ERROR: {e}")


if __name__ == '__main__':
    
    while True:
        try:
            print("#################################")
            transaction = generate_transaction_bank()
            logging.info(transaction)

            producer.produce(
                topic=TOPIC_NAME,
                key=str(transaction["account_id"]),
                value=str(transaction),
                on_delivery=delivery_report
            )
            producer.flush()
            print("#################################")
        except Exception as e:
            logging.error(f"ERROR: {e}")

        logging.info("Generating new event . . .")
        time.sleep(10)

    
    
