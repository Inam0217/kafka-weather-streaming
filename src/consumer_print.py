import json
from kafka import KafkaConsumer

TOPIC = "weather.events"

# Use a fresh group_id so we re-read safely
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:29092",
    group_id="weather-consumer-3",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

print("Listening on topic:", TOPIC)
try:
    for msg in consumer:
        raw = msg.value
        text = raw.decode("utf-8", errors="ignore").strip() if isinstance(raw, (bytes, bytearray)) else str(raw)
        if not text:
            print(f"[{msg.partition}:{msg.offset}] <empty message> — skipped")
            continue
        try:
            rec = json.loads(text)
        except Exception:
            print(f"[{msg.partition}:{msg.offset}] non-JSON — skipped: {text}")
            continue

        city = rec.get("city", "?")
        ts   = rec.get("ts_utc", "")
        temp = rec.get("temp_c", rec.get("temp", "?"))
        main = rec.get("weather_main", "")
        print(f"[{msg.partition}:{msg.offset}] {city} {ts} {main} {temp}°C")
except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
