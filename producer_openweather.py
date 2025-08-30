import os, json, time, datetime as dt
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
assert API_KEY, "Set OPENWEATHER_API_KEY in .env"

# Cities to fetch (you can add more)
CITIES = [
    {"name": "Riyadh",  "lat": 24.7136, "lon": 46.6753},
    {"name": "Jeddah",  "lat": 21.4858, "lon": 39.1925},
    {"name": "Makkah",  "lat": 21.3891, "lon": 39.8579},
]

OW_URL = "https://api.openweathermap.org/data/2.5/weather"
TOPIC = "weather.events"

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def fetch_city(c):
    params = {"lat": c["lat"], "lon": c["lon"], "appid": API_KEY, "units": "metric"}
    r = requests.get(OW_URL, params=params, timeout=15)
    r.raise_for_status()
    d = r.json()
    return {
        "city": c["name"],
        "ts_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "weather_main": (d.get("weather") or [{}])[0].get("main"),
        "weather_desc": (d.get("weather") or [{}])[0].get("description"),
        "temp_c": d.get("main", {}).get("temp"),
        "feels_like_c": d.get("main", {}).get("feels_like"),
        "humidity_pct": d.get("main", {}).get("humidity"),
        "pressure_hpa": d.get("main", {}).get("pressure"),
        "wind_speed_ms": d.get("wind", {}).get("speed"),
        "wind_deg": d.get("wind", {}).get("deg"),
        "clouds_pct": d.get("clouds", {}).get("all"),
        "source": "openweather",
    }

def run_once():
    for c in CITIES:
        try:
            rec = fetch_city(c)
            producer.send(TOPIC, rec)
            producer.flush()
            print("sent:", rec)
        except Exception as e:
            print("ERROR for", c["name"], "->", e)

if __name__ == "__main__":
    # send once per run (manual). Later we can loop every N seconds.
    run_once()
    print("Done.")
