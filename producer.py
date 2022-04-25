from kafka import KafkaProducer

topic = "example"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(bootstrap_servers = KAFKA_SERVER)

producer.send(topic, b'Test message')
producer.flush()
