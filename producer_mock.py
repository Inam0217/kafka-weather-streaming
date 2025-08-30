import json, time, datetime as dt
from kafka import KafkaProducer

# Connect from host/WSL to the broker advertised as PLAINTEXT_HOST://localhost:29092
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

topic = "weather.events"

samples = [
    {"city": "Riyadh", "temp_c": 43.0},
    {"city": "Jeddah", "temp_c": 38.5},
    {"city": "Makkah", "temp_c": 41.2},
]

for rec in samples:
    rec["ts_utc"] = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    producer.send(topic, rec)
    producer.flush()
    print("sent:", rec)
    time.sleep(0.5)

print("Done.")
