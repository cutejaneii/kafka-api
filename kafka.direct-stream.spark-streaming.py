# pip install cassandra-driver
# spark.default.conf import jars : 
#    spark-streaming-kafka-0-8-assembly_2.11-2.0.2.jars
#    spark-cassandra-connector-2.0.0-M2-s_2.11.jars

# Use this command to excute .py file : 
#    spark-submit --packages datastax:spark-cassandra-connector:2.0.0-M2-s_2.11 KafkaToSparkStreamingToCassandra.py

import sys
from pyspark import SparkConf
from pyspark import  SparkContext
from operator import add
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

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
        print(rowRdd)

        rdd = rowRdd.map(lambda w: Row(create_date=w[0], create_user=w[1], raw_data=w[2]))

        print(rdd);

        print('--------------------Start to create Schema--------------------')

        schema = StructType([
                StructField("create_date", StringType(), True),
                StructField("create_user", StringType(), True),
                StructField("raw_data", StringType(), True)
        ])

        wordsDataFrame = spark.createDataFrame(rdd, schema=schema)

        wordsDataFrame.show()

        #SqlContext = SQLContext(sc)

        #df = SqlContext.read\
        #    .format("org.apache.spark.sql.cassandra")\
        #    .options(table="nebula_raw_data", keyspace="production_keyspace")\
        #    .load()

        #df.show()

        wordsDataFrame.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="tabel_name", keyspace="my_keyspace")\
            .save()

    except Exception,ee2:

        print(ee2)
    finally:
        pass

def main():

    try:
        sc = CreateSparkContext()
        ssc = StreamingContext(sc, 10)

        lines = KafkaUtils.createDirectStream(ssc, ["my_topic"], {"metadata.broker.list":"192.168.0.1:9092"})

        counts=lines.map(lambda word: (str(datetime.now()), "api", word[1]))

        counts.foreachRDD(lambda k: saveToCassandra(k, sc, ssc))

        ssc.start()

        ssc.awaitTermination()

    except Exception, e:
        print('error:'+str(e))
    finally:
        pass

if __name__ == "__main__":

    main()

