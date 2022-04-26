from kafka import KafkaConsumer
from pymongo import MongoClient
import json
consumer = KafkaConsumer("MondayMotivation",bootstrap_servers=['localhost:9092'],value_deserializer=lambda x: json.loads(x.decode('utf-8')))
client = MongoClient('localhost:27017')
collection = client.kafkadb.kafkadb
for msg in consumer:
    print(msg)
    msg = msg.value
    collection.insert_one(msg)
consumer.close()
