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
#import json

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement

from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime

#from py4j.protocol import Py4JJavaError

def CreateSparkContext():    

    sparkConf = SparkConf()                                                       \
                         .setAppName("PythonStreaming").setMaster("local[2]") \
                         .set("spark.cassandra.connection.host", "192.168.0.41")
                         #.set("spark.ui.showConsoleProgress", "false") \
                         
    sc = SparkContext(conf = sparkConf)
    
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>master="+sc.appName)
#     SetLogger(sc)
#     SetPath(sc)

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
        #print('ok')
        pass



    # Convert RDDs of the words DStream to DataFrame and run SQL query
def saveToCassandra(rowRdd):
    try:
            # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rowRdd.context.getConf())
        
            # Convert RDD[String] to RDD[Row] to DataFrame     
        #rdd = rowRdd.map(lambda w: Row(time=w[0],input=w[1]))   #Charles=========================================
        
        rdd = rowRdd.map(lambda w: Row(input=w[1],time=w[1]))         

        #rdd = rowRdd.map(lambda w: Row(input=w[0]))
           
        print('-------Start to createDataFrame.....')

	schema = StructType([
		StructField("time", StringType(), True),
		StructField("input", StringType(), True)
	])

	wordsDataFrame = spark.createDataFrame(rdd, schema=schema)

        #wordsDataFrame = spark.createDataFrame(rdd, samplingRatio=0.5)
            # Creates a temporary view using the DataFrame.
#             wordsDataFrame.createOrReplaceTempView("words")
            # Do word count on table using SQL and print it
#             wordCountsDataFrame = \
#                 spark.sql("select word, count(*) as total from words group by word")
#             wordsDataFrame.show()
        
	print(wordsDataFrame)

	print('.........DataFrame ready.....')       
        
        wordsDataFrame.show()


 
        #wordsDataFrame.read.format("org.apache.spark.sql.cassandra").options(table="kafka_test", keyspace="mykeyspace").load().show()
        
        print('.........DataFrame ready2.....')          
        
        wordsDataFrame.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="kafka_test2", keyspace="mykeyspace")\
            .save()

    except Exception,ee2:
        #print('==================================================================saveToCassandra error')
        #print(rdd.isEmpty())
        print(ee2)
    finally:
        pass
        #print('ok')

#counts.foreachRDD(lambda k: saveToCassandra(k))

#print(len(sys.argv))

#def handler(message):
    #records = message.collect()
    #for record in records:
        #producer.send('spark.out', str(record))
        #producer.flush()

def main():
    
    try:
        sc = CreateSparkContext()
        ssc = StreamingContext(sc, 10)
        topics = "Test0221110"

    #KafkaUtils.createDirectStream(ssc, topics, kafkaParams, fromOffsets, keyDecoder, valueDecoder, messageHandler)

        lines = KafkaUtils.createDirectStream(ssc, ["Test0221110"], {"metadata.broker.list": "192.168.0.121:9092"})
        #kvs = KafkaUtils.createStream(ssc, "192.168.0.121:9092", TopicList,  1)
        #kafkaStream = KafkaUtils.createStream(ssc, "192.168.0.121:9092", "topic", {topic: 4})
        #counts=lines.map(lambda word: (word[1], str(datetime.now())))   #Charles=======================================================
        
        counts=lines.map(lambda word: (str(datetime.now()), word[1]))
        
        #counts=lines.map(lambda word: word)
        
        counts.foreachRDD(lambda k: saveToCassandra(k))
        #kafkaStream.foreachRDD(lambda c: print(c))

        ssc.start()
        ssc.awaitTermination()
    
    except Exception, e:
        print('error')
        print(e)
    finally:
        #print('ok')
        pass
    
if __name__ == "__main__":

    main()
