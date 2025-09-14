# Python
import streamlit as st
import time
import pandas as pd
from dotenv import load_dotenv
import ast, os, logging
# kafka
from confluent_kafka import Consumer, KafkaException


# Config Streamlit
st.set_page_config(page_title="Kafka Stream", layout="wide")
st.title("Kafka Transactions Stream")
st.write("Real-time transaction viewer")

# Kafka config
load_dotenv()
IS_LOCAL = True
if IS_LOCAL:
    KAFKA_BOOTSTRAP_ADDRESS = "localhost:29093"
TOPIC_NAME = "test_transactions_topic"

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_ADDRESS,
    'group.id': 'group',
    'auto.offset.reset': 'latest',
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC_NAME])

# Logging
log_format = "[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")

# Container do Streamlit para atualizar em tempo real
container = st.empty()
# Lista para armazenar mensagens
messages_list = []

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        # Converter string do Kafka em dict
        raw_value = msg.value().decode('utf-8')
        try:
            message_value = ast.literal_eval(raw_value)  # converte string Python em dict
        except Exception as e:
            logging.error(f"Erro ao converter mensagem: {e}")
            continue

        # Adiciona a lista
        messages_list.append(message_value)

        # Cria DataFrame para exibir
        df = pd.DataFrame(messages_list)

        # Atualiza Streamlit
        with container.container():
            st.subheader("Latest Transactions")
            st.dataframe(df.tail(5), use_container_width=True)  # últimas 10 mensagens

            """"
            # Exemplo de gráfico: transações aprovadas x não aprovadas
            st.subheader("Transactions Approved vs Declined")
            if 'is_approved' in df.columns:
                chart_data = df['is_approved'].value_counts()
                st.bar_chart(chart_data)
            """
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Transactions Approved vs Declined")
                if 'is_approved' in df.columns:
                    chart_data = df['is_approved'].value_counts()
                    st.bar_chart(chart_data)

            with col2:
                st.subheader("Transaction Types Distribution")
                if 'type' in df.columns:
                    type_counts = df['type'].value_counts()
                    st.bar_chart(type_counts)

            st.subheader("Transactions Over Time (per minute)")
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
                tx_per_min = df.set_index('datetime').resample('1min').size()
                st.line_chart(tx_per_min)

except KeyboardInterrupt:
    logging.info("Shutting down consumer...")

finally:
    consumer.close()
