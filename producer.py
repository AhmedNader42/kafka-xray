from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:29092,localhost:39092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
producer.send("test-topic", 9)
producer.send("test-topic", 10)
producer.send("test-topic", 11)
producer.send("test-topic", 100)
producer.flush()
producer.close()
