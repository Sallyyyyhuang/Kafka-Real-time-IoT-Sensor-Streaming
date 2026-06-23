# processor/stream_processor.py — windowed aggregation + anomaly detection
import sys, os, json, time
from collections import defaultdict, deque
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    config.TOPIC_READINGS,
    bootstrap_servers=config.BOOTSTRAP_SERVERS,
    group_id="sensor-processor",          # the consumer GROUP — run me twice!
    auto_offset_reset="earliest",
    enable_auto_commit=False,             # we commit offsets ourselves
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
)
producer = KafkaProducer(
    bootstrap_servers=config.BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

windows = defaultdict(lambda: {"count": 0, "sum": 0.0, "min": None, "max": None})
history = defaultdict(lambda: deque(maxlen=30))   # rolling values per device
window_start = time.time()

def flush_windows(now):
    """Emit one aggregate per device for the window that just closed."""
    for dev, w in windows.items():
        if w["count"] == 0:
            continue
        agg = {
            "device_id": dev, "window_end": now, "count": w["count"],
            "avg": round(w["sum"] / w["count"], 2), "min": w["min"], "max": w["max"],
        }
        producer.send(config.TOPIC_AGGREGATES, key=dev, value=agg)
    windows.clear()

def check_anomaly(r):
    stype, val, dev = r["sensor_type"], r["value"], r["device_id"]
    lo, hi = config.THRESHOLDS.get(stype, (float("-inf"), float("inf")))
    reason = None
    if val < lo or val > hi:
        reason = f"{val} outside [{lo}, {hi}]"
    else:
        h = history[dev]
        if len(h) >= 10:
            mean = sum(h) / len(h)
            var = sum((x - mean) ** 2 for x in h) / len(h)
            std = var ** 0.5
            if std > 0 and abs(val - mean) > 3 * std:
                reason = f"{val} is >3σ from mean {round(mean,2)}"
    history[dev].append(val)
    if reason:
        producer.send(config.TOPIC_ALERTS, key=dev,
                      value={"device_id": dev, "room": r["room"],
                             "sensor_type": stype, "value": val,
                             "reason": reason, "ts": r["ts"]})
        print(f"  ALERT {dev}: {reason}")

print("processor running — Ctrl-C to stop")
try:
    for msg in consumer:
        r = msg.value
        w = windows[r["device_id"]]
        w["count"] += 1
        w["sum"]   += r["value"]
        w["min"] = r["value"] if w["min"] is None else min(w["min"], r["value"])
        w["max"] = r["value"] if w["max"] is None else max(w["max"], r["value"])
        check_anomaly(r)

        now = time.time()
        if now - window_start >= config.WINDOW_SECONDS:
            flush_windows(now)
            consumer.commit()          # only commit after a window is emitted
            window_start = now
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    producer.close()
