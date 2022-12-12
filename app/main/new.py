import findspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.streaming import DataStreamReader

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

userSchema = StructType().add("client_ac_no", "string").add("trade_price", "string")

spark_dataframe = spark \
    .readStream \
    .schema(userSchema) \
    .format("csv") \
    .load(csv_input_path)


spark_dataframe.select(to_json(struct("client_ac_no", "trade_price")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("topic", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .start()

spark_dataframe.show()

print('# ----------------------- Topic written to kafka using pyspark ---------------------------- #')
#
# kafka_df = spark.read \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_brokers) \
#     .option("failOnDataLoss", "false") \
#     .option("subscribe", kafka_topic) \
#     .option("includeHeaders", "true") \
#     .option("spark.streaming.kafka.maxRatePerPartition", "50") \
#     .load()
#
# kafka_df_new = kafka_df.selectExpr("CAST(value AS STRING)")
#
# labels = [
#     ("client_ac_no", StringType()),
#     ("trade_price", StringType())
# ]
#
# schema = StructType([StructField(x[0], x[1], True) for x in labels])
#
# new_df = kafka_df_new.select(from_json(col("value"), schema))
#
# new_df.show()
