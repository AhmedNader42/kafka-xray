from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:29092,localhost:39092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    producer.send("test-topic-1", 9)
    producer.send("test-topic-1", 10)
    producer.send("test-topic-1", 11)
    producer.send("test-topic-1", 100)
    producer.flush()

    time.sleep(5)

producer.close()
