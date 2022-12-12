from time import sleep
from json import dumps
from kafka import KafkaProducer


def kafka_producer(kafka_brokers, kafka_topic, pyspark_df):
    producer = KafkaProducer(
        bootstrap_servers=kafka_brokers,
        value_serializer=lambda x:
        dumps(x).encode('utf-8')
    )

    for i, row in pyspark_df.iterrows():
        # data = {row['client_ac_no']: row['trade_price']}
        data = row.to_dict()
        producer.send(kafka_topic, value=data)
        # sleep(2)
