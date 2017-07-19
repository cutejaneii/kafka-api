## Kafka-http ##

using Kafka-python.
To allow users to insert json data into Kafka, then save into Cassandra by using spark streaming.

### Insert data into Kafka ###

**kafka-api-with-topic.py**

User call API with topic name and data. User decide data should insert into which topic.

**kafka-api.py**

User call API with data which need to be insert into Kafka.
Kafka topic is defined in REST API, user cannot change it.


### Get Kafka message and save into Cassandra ###

**kafka.stream.spark-streaming.py**

1. Method : KafkaUtils.createStream
2. zookeeper offset will be updated after get topic message by using KafkaUtils.createStream. 
3. Do not implement any error handle function after get message successfully, so if function fail when saving to Cassandra, the message will miss.

**kafka.direct-stream.spark-streaming.py**

1. Method : KafkaUtils.createDirectStream
2. zookeeper offset will NOT be updated after get topic message by using KafkaUtils.createDirectStream. 


**kafka.stream.spark-streaming-handle-error.py**

1. Method : KafkaUtils.createStream
2. zookeeper offset will be updated after get topic message by using KafkaUtils.createStream. 
3. Implement error handle function after get message, so if function fail when saving to Cassandra, will save the error messages into ANOTHER TOPIC. 

