import json
import ast
import pandas as pd

from kafka import KafkaConsumer
from os import path as os_path


def kafka_consumer(kafka_brokers, kafka_topic, csv_output_path):
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_brokers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group'
    )

    print('\n# ------------------- Consumer - Received messages ------------------- #')
    for message in consumer:
        message = ast.literal_eval(message.value.decode('utf-8'))
        read_message_df = pd.DataFrame(message, index=[0])
        read_message_df.to_csv(csv_output_path, mode='a', header=not os_path.exists(csv_output_path), index=False)
        print(f'Message read -> {message}')
        # message = json.loads(message.decode('utf-8'))
