# encoding=UTF-8
#!flask/bin/python

from flask import Flask, request, json
from kafka import KafkaProducer
from kafka.errors import KafkaError

app = Flask(__name__)

@app.route('/', methods=['GET'])

def index():
	return "Hello, this is MyKafkaApi."

@app.route('/add', methods=['POST'])

def add():
	print(type(request.json['data']))
	print(type(request.json['data'].encode('utf-8')))
	print(request.json)
	print(request.json['data'])
	producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])
	producer.send('topic_name', request.json['data'].encode('utf-8'))	
	producer.flush()
	return "add Data~"
        
if __name__ ==  '__main__':
	app.run(debug=True)  
