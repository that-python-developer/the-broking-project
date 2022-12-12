import findspark
import pandas as pd

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


def final_price_calculations(pandas_df):
    try:
        pandas_df[['trade_price', 'trade_qty']] = pandas_df[['trade_price', 'trade_qty']].apply(pd.to_numeric)
    except Exception as e:
        print('ERROR : Some value in the columns trade_price or trade_qty is non-numeric!!')
        raise e
    pandas_df['charges'] = pandas_df['trade_price'] * pandas_df['trade_qty'] * 0.1
    pandas_df = pandas_df.round({'charges': 2})
    pandas_df['total'] = pandas_df['charges'] + pandas_df['trade_price']
    return pandas_df


def pyspark_read_data(csv_input_path):
    findspark.init()

    conf = SparkConf().setMaster("local").setAppName("Broking")
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.getOrCreate()

    spark_dataframe = spark \
        .read \
        .options(delimiter=",", header=True, inferSchema='False') \
        .csv(csv_input_path)

    pandas_df = spark_dataframe.toPandas()

    pandas_df = final_price_calculations(pandas_df)

    pandas_df = pandas_df[['client_ac_no', 'trade_price', 'charges', 'total']]
    pandas_df = pandas_df.head()

    return pandas_df
