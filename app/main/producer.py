import findspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct


csv_input_path = 'D:\\the-broking-project\\app\data\\test.csv'
csv_output_path = 'D:\\the-broking-project\\app\data\\output.csv'
kafka_brokers = "localhost:9092"
kafka_topic = "TestTopic"

findspark.init()

scala_version = '2.12'
spark_version = '3.3.0'
# TODO: Ensure match above values match the correct versions
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]
spark = SparkSession.builder\
   .master("local")\
   .appName("kafka-example")\
   .config("spark.jars.packages", ",".join(packages))\
   .getOrCreate()

spark_dataframe = spark \
    .read \
    .options(delimiter=",", header=True, inferSchema='False') \
    .csv(csv_input_path)

spark_dataframe.select(to_json(struct("client_ac_no", "trade_qty", "trade_price")).alias("value")) \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("failOnDataLoss", "false") \
    .option("topic", kafka_topic) \
    .save()

spark_dataframe.show()

print('# ----------------------- Topic written to kafka using pyspark ---------------------------- #')
