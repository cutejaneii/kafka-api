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

from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime

def CreateSparkContext():    

    sparkConf = SparkConf()                                                       \
                         .setAppName("PythonStreaming").setMaster("local[2]") \
                         .set("spark.cassandra.connection.host", "x.x.x.x")
                         
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
def saveToCassandra(rowRdd):
    try:
            # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rowRdd.context.getConf())
        
            # Convert RDD[String] to RDD[Row] to DataFrame     
        
        rdd = rowRdd.map(lambda w: Row(input=w[1],time=w[1])) 
	
        print('-------Start to createDataFrame.....')

	schema = StructType([
		StructField("time", StringType(), True),
		StructField("input", StringType(), True)
	])

	wordsDataFrame = spark.createDataFrame(rdd, schema=schema)
       
	print(wordsDataFrame)

	print('.........DataFrame ready.....')       
        
        wordsDataFrame.show()

        print('.........DataFrame ready2.....')          
        
        wordsDataFrame.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="kafka_test2", keyspace="mykeyspace")\
            .save()

    except Exception,ee2:

        print(ee2)
    finally:
        pass

def main():
    
    try:
        sc = CreateSparkContext()
        ssc = StreamingContext(sc, 10)
        topics = "Test0221110"

        lines = KafkaUtils.createDirectStream(ssc, ["Test0221110"], {"metadata.broker.list": "127.0.0.1:9092"})

        counts=lines.map(lambda word: (str(datetime.now()), word[1]))
               
        counts.foreachRDD(lambda k: saveToCassandra(k))

        ssc.start()
	
        ssc.awaitTermination()
    
    except Exception, e:
        print('error:'+str(e))
    finally:
        pass
    
if __name__ == "__main__":

    main()
