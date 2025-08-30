import os, json
from datetime import datetime
import mysql.connector
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()
host = os.getenv("MYSQL_HOST","localhost")
port = int(os.getenv("MYSQL_PORT","3306"))
user = os.getenv("MYSQL_USER","kafka")
pwd  = os.getenv("MYSQL_PASSWORD","")
dbnm = os.getenv("MYSQL_DB","weather_streaming")

print(f"Connecting to MySQL as {user}@{host}:{port}, db={dbnm}")
db = mysql.connector.connect(host=host, port=port, user=user, password=pwd, database=dbnm)
cursor = db.cursor()

# helper: convert ISO8601 '2025-08-28T11:56:27Z' -> '2025-08-28 11:56:27'
def iso_to_mysql_datetime(ts: str):
    if not ts: 
        return None
    try:
        ts = ts.strip()
        if ts.endswith("Z"):
            ts = ts[:-1]  # drop Z
        # '2025-08-28T11:56:27' -> python dt
        dt = datetime.fromisoformat(ts.replace(" ", "T"))  # supports both ' ' and 'T'
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

consumer = KafkaConsumer(
    "weather.events",
    bootstrap_servers="localhost:29092",
    group_id="weather-mysql-2",      # new group to avoid old junk
    auto_offset_reset="latest",       # start from new messages only
    enable_auto_commit=True,
)

insert_sql = """
INSERT INTO weather_stream
(city, ts_utc, weather_main, weather_desc, temp_c, feels_like_c,
 humidity_pct, pressure_hpa, wind_speed_ms, wind_deg, clouds_pct, source, raw_json)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

print("Consumer → MySQL started...")
for msg in consumer:
    try:
        raw = msg.value
        text = raw.decode("utf-8", errors="ignore").strip() if isinstance(raw, (bytes, bytearray)) else str(raw)
        if not text:
            print(f"[{msg.partition}:{msg.offset}] <empty> skipped")
            continue
        try:
            rec = json.loads(text)
        except Exception:
            print(f"[{msg.partition}:{msg.offset}] non-JSON skipped: {text[:60]}")
            continue

        ts_mysql = iso_to_mysql_datetime(rec.get("ts_utc"))
        values = (
            rec.get("city"),
            ts_mysql,
            rec.get("weather_main"),
            rec.get("weather_desc"),
            rec.get("temp_c"),
            rec.get("feels_like_c"),
            rec.get("humidity_pct"),
            rec.get("pressure_hpa"),
            rec.get("wind_speed_ms"),
            rec.get("wind_deg"),
            rec.get("clouds_pct"),
            rec.get("source"),
            json.dumps(rec),
        )
        cursor.execute(insert_sql, values)
        db.commit()
        print("inserted:", rec.get("city"), ts_mysql)
    except Exception as e:
        print("ERROR:", e)
