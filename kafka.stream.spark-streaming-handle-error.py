import sys
from pyspark import SparkConf
from pyspark import  SparkContext
from operator import add
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

from kafka import KafkaProducer

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
                         .set("spark.cassandra.connection.host", "192.168.0.2")

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

        print('get spark...')
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rowRdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rdd = rowRdd.map(lambda w: Row(create_date=w[0], create_user=w[1], raw_data=w[2]))

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
            .options(table="table_name", keyspace="my_keyspace")\
            .save()

    except Exception,ee2:
        HandleError(rowRdd)
        print(ee2)
    finally:
        pass

def HandleError(rowRdd):
    try:
        print('================================handle error ================================')

        #Convert pyspark.RDD -> list
        lstRowRdd = rowRdd.collect()

        if lstRowRdd > 0:
            producer = KafkaProducer(bootstrap_servers='192.168.0.1:9092')
            for iRowRdd in lstRowRdd:
                input_data = topic + ':' + str(iRowRdd[2])
                producer.send(errortopic, input_data)
                print('error topic:' + input_data)
            producer.flush()

    except Exception, e:
        print('error2:'+str(e))
    finally:
        pass

def main():
    global topic
    topic = "topic_name"
    global errortopic
    errortopic = 'error_topic_data'

    sc = CreateSparkContext()
    ssc = StreamingContext(sc, 10)
    try:
        kafka_stream = KafkaUtils.createStream(ssc, "192.168.0.1:2181", "spark-streaming-consumer", {topic:12})

        raw = kafka_stream.flatMap(lambda kafkaS: [kafkaS])

        lines = raw.flatMap(lambda xs: xs[1].split(","))

        counts = lines.map(lambda word: (str(datetime.now()), "api", word))

        counts.foreachRDD(lambda k: saveToCassandra(k, sc, ssc))

    except Exception, e:
        print('error :'+str(e))
    finally:
        pass
        ssc.start()
        ssc.awaitTermination()

if __name__ == "__main__":
    main()
