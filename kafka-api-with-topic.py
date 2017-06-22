# encoding=UTF-8
#!flask/bin/python

from flask import Flask, request, json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

app = Flask(__name__)

@app.route('/', methods=['GET'])

def index():
	return "Hello, this is MyKafkaApi."

@app.route('/input', methods=['POST'])

def add():
	
	input_data = request.json['input_data'].encode('utf-8')
	topic = request.json['topic'].encode('utf-8')
	
        producer = KafkaProducer(bootstrap_servers=['Your kafka ip:9092'])
        #producer = KafkaProducer(bootstrap_servers=[kafka_service_ip_port])
	producer.send(topic, input_data)	
	producer.flush()
	return "Success!"
        
if __name__ ==  '__main__':
	app.run(debug=True)      
