from kafka import KafkaConsumer

topic = "example"
con = KafkaConsumer(topic)
for i in con:
	print(i)
