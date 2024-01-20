from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for number in range(2001, 4001):
    producer.send('page', str(number).encode())
    producer.flush()
