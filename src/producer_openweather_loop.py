import os, json, time, datetime as dt, requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
assert API_KEY, "Set OPENWEATHER_API_KEY in .env"

INTERVAL_SEC = int(os.getenv("FETCH_INTERVAL", "300"))  # default 5 min
CITIES = [
    {"name": "Riyadh", "lat": 24.7136, "lon": 46.6753},
    {"name": "Jeddah", "lat": 21.4858, "lon": 39.1925},
    {"name": "Makkah", "lat": 21.3891, "lon": 39.8579},
]

OW_URL = "https://api.openweathermap.org/data/2.5/weather"
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def fetch(lat, lon):
    r = requests.get(OW_URL, params={"lat": lat, "lon": lon, "appid": API_KEY, "units": "metric"}, timeout=10)
    r.raise_for_status()
    return r.json()

def send_record(city, data):
    rec = {
        "city": city["name"],
        "ts_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "weather_main": (data.get("weather") or [{}])[0].get("main"),
        "weather_desc": (data.get("weather") or [{}])[0].get("description"),
        "temp_c": data.get("main", {}).get("temp"),
        "feels_like_c": data.get("main", {}).get("feels_like"),
        "humidity_pct": data.get("main", {}).get("humidity"),
        "pressure_hpa": data.get("main", {}).get("pressure"),
        "wind_speed_ms": data.get("wind", {}).get("speed"),
        "wind_deg": data.get("wind", {}).get("deg"),
        "clouds_pct": data.get("clouds", {}).get("all"),
        "source": "openweather",
    }
    producer.send("weather.events", rec)
    producer.flush()
    print("sent:", rec["city"], rec["ts_utc"], rec["temp_c"])

if __name__ == "__main__":
    print(f"Looping every {INTERVAL_SEC}s. Ctrl+C to stop.")
    while True:
        start = time.time()
        for c in CITIES:
            try:
                d = fetch(c["lat"], c["lon"])
                send_record(c, d)
            except Exception as e:
                print("ERROR for", c["name"], "->", e)
        sleep_for = max(0, INTERVAL_SEC - (time.time() - start))
        time.sleep(sleep_for)
