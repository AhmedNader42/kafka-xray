from kafka import KafkaConsumer
import math

consumer = KafkaConsumer(
    "test-topic",
    bootstrap_servers="localhost:29092,localhost:39092",
    # auto_offset_reset="earliest",
)

minimum = 0.0
maximum = 0.0
moving_avg = 0.0
stream_running_length = 0.0
stream_total_sum = 0.0

for message in consumer:
    print("Attempting to read from topic")
    p = float(message.value.decode("utf-8"))

    stream_running_length += 1
    minimum = min(minimum, p)
    maximum = max(maximum, p)
    stream_total_sum += p

    moving_avg = p / stream_running_length

    print(p)
    print(minimum)
    print(maximum)
    print(stream_running_length)
    print(stream_total_sum)
    print(moving_avg)
