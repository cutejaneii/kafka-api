import sys
from pyspark import SparkConf
from pyspark import  SparkContext
from operator import add
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement

from pyspark.sql import Row, SparkSession, SQLContext, types
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime

def CreateSparkContext():

    sparkConf = SparkConf()                                                       \
                         .setAppName("PythonStreaming").setMaster("local[2]") \
                         .set("spark.cassandra.connection.host", "192.168.0.1")

    sc = SparkContext(conf = sparkConf)
    return (sc)

def getSparkSessionInstance(sparkConf):
    try:
        if ('sparkSessionSingletonInstance' not in globals()):
            print('in')
            globals()['sparkSessionSingletonInstance'] = SparkSession\
                .builder\
                .config(conf=sparkConf)\
                .master('local').getOrCreate()
        return globals()['sparkSessionSingletonInstance']
    except Exception,ee:
        print(ee)
    finally:
        pass


    # Convert RDDs of the words DStream to DataFrame and run SQL query
def saveToCassandra(rowRdd, sc, ssc):
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rowRdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame

        print('--------------------Start to createDataFrame--------------------')

        rdd = rowRdd.map(lambda w: Row(create_date=w[0], create_user=w[1], raw_data=w[2]))

        schema = StructType([
            StructField("create_date", StringType(), True),
            StructField("create_user", StringType(), True),
            StructField("raw_data", StringType(), True)
        ])

        wordsDataFrame = spark.createDataFrame(rdd, schema=schema)

        wordsDataFrame.show()

        print('------------------Start to write save to cassandra------------------')

        wordsDataFrame.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="table_1", keyspace="my_keyspace")\
            .save()

    except Exception,ee2:

        print(ee2)
    finally:
        pass

def SetParameters():
    global topic
    topic = "my_topic"

def main():

    try:
        SetParameters()
        sc = CreateSparkContext()
        ssc = StreamingContext(sc, 5)

        kafka_stream = KafkaUtils.createStream(ssc, "192.168.0.1:2181", "topic", {topic:12})

        raw = kafka_stream.flatMap(lambda kafkaS: [kafkaS])

        lines = raw.flatMap(lambda xs: xs[1].split(","))

        counts = lines.map(lambda word: (str(datetime.now()), "api", word))

        counts.foreachRDD(lambda k: saveToCassandra(k, sc, ssc))

        ssc.start()

        ssc.awaitTermination()

    except Exception, e:
        print('error:'+str(e))
    finally:
        pass

if __name__ == "__main__":

    main()
