from app.old.kafka_producer import kafka_producer
from app.old.kafka_consumer import kafka_consumer
from app.old.pyspark_read_data import pyspark_read_data


if __name__ == '__main__':
    csv_input_path = '/app/data/test.csv'
    csv_output_path = 'D:\\the-broking-project\\app\data\\output.csv'
    kafka_brokers = "localhost:9092"
    kafka_topic = "TestTopic"

    pyspark_df = pyspark_read_data(csv_input_path)
    print('\n# ------------------- Completed - Reading file ------------------- #')

    kafka_producer(kafka_brokers, kafka_topic, pyspark_df)
    print('\n# ------------------- Completed - Producing messages ------------------- #')

    kafka_consumer(kafka_brokers, kafka_topic, csv_output_path)
