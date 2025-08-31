# Apache Kafka
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
# Basic Python
import os, time
from dotenv import load_dotenv
import logging
# Internals
from services.financial_data import generate_transaction_bank

# Globals
log_format = "[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
load_dotenv()

# Servers ports and Kafka Settings
SCHEMA_REGISTRY_PORT = os.getenv("SCHEMA_REGISTRY_PORT")
KAFKA_INTERNAL_PORT = os.getenv("KAFKA_INTERNAL_PORT")

TOPIC_NAME =  "transactions_topic"
print("PORTS: KAFKA_INTERNAL_PORT: ", KAFKA_INTERNAL_PORT, " SCHEMA_REGISTRY_PORT: ", SCHEMA_REGISTRY_PORT)

schema_str = """
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.example.transactions",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "account_id", "type": "string"},
    {"name": "type", "type": "string"},
    {"name": "card_category", "type": "string"},
    {"name": "card_number", "type": "string"},
    {"name": "is_approved", "type": "boolean"},
    {"name": "datetime", "type": "string"}
  ]
}
"""

try:
    logging.info("Trying to create a schema ...")
    schema_registry_conf = {'url': f'http://schema-registry-server:{SCHEMA_REGISTRY_PORT}'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Avro Serializer
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str
    )

    # Necessary for serializing the key
    string_serializer = StringSerializer("utf_8")

    # Producer settings
    producer_conf = {
        "bootstrap.servers": f'kafka:{KAFKA_INTERNAL_PORT}',
        "key.serializer": string_serializer,  # opcional, se quiser chave string
        "value.serializer": avro_serializer
    }

    producer = SerializingProducer(producer_conf)

    def delivery_report(err, msg):
        """ Delivery Callback """
        if err is not None:
            logging.error(f"Error when trying to send the message: {err}")
        else:
            logging.info(f"Mesagen sent to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")
        
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
                value=transaction,
                on_delivery=delivery_report
            )
            producer.flush()
            print("#################################")
        except Exception as e:
            logging.error(f"ERROR: {e}")

        logging.info("Generating new event . . .")
        time.sleep(10)

    
    
