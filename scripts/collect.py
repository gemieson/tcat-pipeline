"""Collect TCAT live data to disk. Crude on purpose — this is a bucket
under a leak while the real pipeline gets built."""
import json
import os
import time
from datetime import datetime, timezone

import requests

BASE = "https://realtimetcatbus.availtec.com/InfoPoint/rest"
VEHICLES_EVERY = 30      # seconds
ALERTS_EVERY = 300       # 5 minutes

os.makedirs("data/samples/vehicles", exist_ok=True)
os.makedirs("data/samples/alerts", exist_ok=True)


def stamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def grab(path, folder, label):
    """Fetch one endpoint, write it to disk. Never raises."""
    try:
        r = requests.get(f"{BASE}/{path}", timeout=20)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        # A transit API times out sometimes. Log it, keep the loop alive.
        print(f"  [warn] {label}: {e}")
        return None

    with open(f"data/samples/{folder}/{label}_{stamp()}.json", "w") as f:
        json.dump(data, f)
    return data


def count_vehicles(routes):
    if not routes:
        return 0
    return sum(len(r.get("Vehicles") or []) for r in routes)


if __name__ == "__main__":
    print("collecting — Ctrl+C to stop")
    last_alerts = 0

    while True:
        started = time.time()

        routes = grab("Routes/GetVisibleRoutes", "vehicles", "routes")
        n = count_vehicles(routes)

        if time.time() - last_alerts >= ALERTS_EVERY:
            alerts = grab("PublicMessages/GetCurrentMessages", "alerts", "alerts")
            last_alerts = time.time()
            extra = f" | {len(alerts)} alerts" if alerts is not None else ""
        else:
            extra = ""

        print(f"[{datetime.now():%H:%M:%S}] {n} vehicles{extra}")

        time.sleep(max(0, VEHICLES_EVERY - (time.time() - started)))