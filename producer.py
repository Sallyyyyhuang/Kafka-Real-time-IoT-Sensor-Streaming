import json
from kafka import KafkaProducer


def on_success(record_metadata):
    print(f"Sent to {record_metadata.topic} "
          f"partition={record_metadata.partition} "
          f"offset={record_metadata.offset}")


def on_error(exc):
    print(f"Failed to send: {exc}")


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    # Serialize values to JSON bytes automatically
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    # Serialize keys to bytes automatically
    key_serializer=lambda k: k.encode('utf-8'),
)

# Send 9 messages with different user keys
# Kafka hashes the key to decide which partition to use,
# so the same key always goes to the same partition
users = [f'user-{i}' for i in range(1, 11)]  # user-1 through user-10
events = ['login', 'purchase', 'logout']

for user in users:
    for event in events:
        future = producer.send(
            topic='my-topic',
            key=user,
            value={'user': user, 'event': event}
        )
        future.add_callback(on_success)
        future.add_errback(on_error)

producer.flush()
producer.close()

print("All messages sent!")
