import json
from kafka import KafkaConsumer

# Same group_id as consumer.py — Kafka will split the 3 partitions
# between this consumer and consumer.py (e.g. each gets 1-2 partitions)
consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers='localhost:9092',
    group_id='my-consumer-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
)

print("Consumer 2 waiting for messages...")

try:
    for message in consumer:
        print(f"[consumer2] partition={message.partition} offset={message.offset} "
              f"key={message.key} value={message.value}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
