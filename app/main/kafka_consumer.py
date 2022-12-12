import json
import ast
import pandas as pd

from kafka import KafkaConsumer
from os import path as os_path


csv_output_path = 'D:\\the-broking-project\\app\data\\output.csv'

kafka_brokers = "localhost:9092"
kafka_topic = "TestTopic"

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_brokers,
    auto_offset_reset='latest',
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
