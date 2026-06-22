import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers='localhost:9092',
    group_id='my-consumer-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    # Deserialize JSON bytes back to a Python dict automatically
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
)

print("Waiting for messages...")

try:
    for message in consumer:
        print(f"partition={message.partition} offset={message.offset} "
              f"key={message.key} value={message.value}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
