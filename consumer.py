from kafka import KafkaConsumer
import statistics


TOPIC_NAME = "test-topic-1"
BOOTSTRAP_SERVERS = ["localhost:29092", "localhost:39092"]
NUMERIC_THRESHOLD = 1.5

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
)


total_stats = {
    "minimum": 0.0,
    "maximum": 0.0,
    "moving_avg": 0.0,
    "stream_running_length": 0.0,
    "stream_total_sum": 0.0,
}

for message in consumer:
    current_record_stats = total_stats

    print("Attempting to read from topic")
    reading = float(message.value.decode("utf-8"))

    current_record_stats["stream_running_length"] += 1
    current_record_stats["minimum"] = min(current_record_stats["minimum"], reading)
    current_record_stats["maximum"] = max(current_record_stats["maximum"], reading)
    current_record_stats["stream_total_sum"] += reading

    current_record_stats["moving_avg"] = (
        reading / current_record_stats["stream_running_length"]
    )

    if (
        current_record_stats["moving_avg"]
        > NUMERIC_THRESHOLD * total_stats["moving_avg"]
        or current_record_stats["moving_avg"]
        < NUMERIC_THRESHOLD * total_stats["moving_avg"]
    ):
        print(
            f"ALERT! Record exceeds specified threshold {NUMERIC_THRESHOLD} From average {total_stats['moving_avg']}"
        )
        continue

    total_stats["stream_running_length"] += 1
    total_stats["minimum"] = min(total_stats["minimum"], reading)
    total_stats["maximum"] = max(total_stats["maximum"], reading)
    total_stats["stream_total_sum"] += reading

    total_stats["moving_avg"] = reading / total_stats["stream_running_length"]

    print(f"  Reading: {reading}")
    print(f"  Minimum: {total_stats['minimum']}")
    print(f"  Maximum: {total_stats['maximum']}")
    print(f"  Length: {total_stats['stream_running_length']}")
    print(f"  Sum: {total_stats['stream_total_sum']}")
    print(f"  Average: {total_stats['moving_avg']:.2f}")

consumer.close()
