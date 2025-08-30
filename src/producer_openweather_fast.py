import os, json, datetime as dt, requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
assert API_KEY, "Set OPENWEATHER_API_KEY in .env"

OW_URL = "https://api.openweathermap.org/data/2.5/weather"
params = {"lat": 24.7136, "lon": 46.6753, "appid": API_KEY, "units": "metric"}

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def send_once():
    r = requests.get(OW_URL, params=params, timeout=6)
    r.raise_for_status()
    d = r.json()
    rec = {
        "city": "Riyadh",
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
    producer.send("weather.events", rec)
    producer.flush()
    print("sent:", rec)

if __name__ == "__main__":
    send_once()
    print("Done.")
