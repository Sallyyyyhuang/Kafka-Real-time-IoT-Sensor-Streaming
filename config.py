# config.py — shared settings for every stage
BOOTSTRAP_SERVERS = "localhost:29092"

TOPIC_READINGS   = "sensor-readings"
TOPIC_AGGREGATES = "sensor-aggregates"
TOPIC_ALERTS     = "sensor-alerts"

READINGS_PARTITIONS = 3      # lets up to 3 processors share the load
DERIVED_PARTITIONS  = 1

WINDOW_SECONDS = 10          # tumbling-window size for aggregation

# Simulated fleet: (device_id, room, sensor_type, unit, baseline)
DEVICES = [
    ("temp-a",  "kitchen",     "temperature", "C",   21.0),
    ("temp-b",  "bedroom",     "temperature", "C",   20.0),
    ("hum-a",   "kitchen",     "humidity",    "%",   45.0),
    ("hum-b",   "bathroom",    "humidity",    "%",   60.0),
    ("co2-a",   "office",      "co2",         "ppm", 600.0),
    ("co2-b",   "living-room", "co2",         "ppm", 550.0),
]

# Hard limits — a reading outside these fires an alert
THRESHOLDS = {
    "temperature": (10.0, 30.0),
    "humidity":    (20.0, 80.0),
    "co2":         (350.0, 1200.0),
}
