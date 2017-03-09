# encoding=UTF-8
#!flask/bin/python

from flask import Flask, request, json
from kafka import KafkaProducer
from kafka.errors import KafkaError

app = Flask(__name__)

@app.route('/', methods=['GET'])

def index():
	producer2 = KafkaProducer(bootstrap_servers=['192.168.0.121:9092'])
	producer2.send('Test0220', b'1500')	
	producer2.flush()
	return "Index"

@app.route('/add', methods=['POST'])

def add():
	print(type(request.json['data1']))
	print(type(request.json['data1'].encode('utf-8')))
	print(request.json)
	print(request.json['data1'])
	producer = KafkaProducer(bootstrap_servers=['192.168.0.121:9092'])
	producer.send('Test0220', request.json['data1'].encode('utf-8'))	
	producer.flush()
	return "add Data~"
        
if __name__ ==  '__main__':
	app.run(debug=True)      
        
#import json
#producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('UTF-8'))
#producer.send('aa', {'key': 'value'})

