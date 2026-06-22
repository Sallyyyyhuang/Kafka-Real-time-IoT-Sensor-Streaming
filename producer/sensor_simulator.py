# producer/sensor_simulator.py — emits realistic IoT readings into Kafka
import sys, os, json, time, math, random
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=config.BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",          # wait for the broker to persist => no silent data loss
)

def reading(device_id, room, sensor_type, unit, baseline, t):
    # diurnal sine wave + noise; ~3% chance of an anomalous spike
    daily = math.sin(t / 30.0) * 2.0
    noise = random.gauss(0, 0.5)
    value = baseline + daily + noise
    if random.random() < 0.03:
        value += random.choice([-1, 1]) * baseline * 0.5   # spike
    return {
        "device_id": device_id, "room": room, "sensor_type": sensor_type,
        "unit": unit, "value": round(value, 2), "ts": time.time(),
    }

print("simulating sensors — Ctrl-C to stop")
t = 0
try:
    while True:
        for (dev, room, stype, unit, base) in config.DEVICES:
            msg = reading(dev, room, stype, unit, base, t)
            producer.send(config.TOPIC_READINGS, key=dev, value=msg)
        producer.flush()
        print(f"sent {len(config.DEVICES)} readings (t={t})")
        t += 1
        time.sleep(1.0)
except KeyboardInterrupt:
    pass
finally:
    producer.close()
