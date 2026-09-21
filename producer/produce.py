"""Poll the TCAT Avail feed and publish one message per event.

Two streams, two cadences, one process:
  vehicles   every 30s  -> tcat.vehicle_positions.v1   (keyed by VehicleId)
  departures every 60s  -> tcat.stop_departures.v1     (keyed by StopId)

Payloads are published untouched. All cleaning happens in dbt, where it is
version-controlled, tested and re-runnable.
"""
import json, os, time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

BASE = "https://realtimetcatbus.availtec.com/InfoPoint/rest"
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "tcat.vehicle_positions.v1")
DEPARTURE_TOPIC = os.getenv("DEPARTURE_TOPIC", "tcat.stop_departures.v1")

VEHICLE_EVERY = int(os.getenv("VEHICLE_SECONDS", "30"))
DEPARTURE_EVERY = int(os.getenv("DEPARTURE_SECONDS", "60"))
VERSION = "1.2.0"

# Same timepoint stops the bash collector watches. Keep the two lists in sync.
DEPARTURE_STOPS = [100, 165, 172, 1343, 1344, 1513, 1541, 1715, 1534, 339,
                   349, 374, 541, 549, 665, 3588, 3579, 1774, 4783, 2631, 996]

producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BROKER", "localhost:9092"),
    "linger.ms": 100,
})


def get(url: str):
    """Fetch and parse, or return None. A transit API will time out; log and
    keep going -- that is the difference between a script and a service."""
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, ValueError) as e:
        print(f"[warn] {url} failed: {e}", flush=True)
        return None


def poll_vehicles() -> int:
    routes = get(f"{BASE}/Routes/GetVisibleRoutes")
    if routes is None:
        return 0

    ingested_at = datetime.now(timezone.utc).isoformat()
    count = 0

    for route in routes:
        for vehicle in (route.get("Vehicles") or []):
            envelope = {
                "ingested_at": ingested_at,
                "producer_version": VERSION,
                "route_id": route["RouteId"],
                "route_record_id": route.get("RouteRecordId"),
                "payload": vehicle,
            }
            producer.produce(
                VEHICLE_TOPIC,
                key=str(vehicle["VehicleId"]).encode(),
                value=json.dumps(envelope).encode(),
            )
            count += 1

    producer.flush()
    return count


def poll_departures() -> int:
    """One call per stop.

    Avail wraps the payload: a one-element list holding the stop, with
    RouteDirections nested inside, and Departures inside those. Older/other
    deployments return the bare RouteDirections array, so handle both.
    """
    ingested_at = datetime.now(timezone.utc).isoformat()
    count = 0

    for stop_id in DEPARTURE_STOPS:
        body = get(f"{BASE}/StopDepartures/Get/{stop_id}")
        if not body:
            continue

        for entry in body:
            # wrapper shape -> pull the nested list; bare shape -> use as-is
            route_dirs = entry.get("RouteDirections")
            if route_dirs is None:
                route_dirs = [entry]
                stop_record_id = None
                stop_last_updated = None
            else:
                stop_record_id = entry.get("StopRecordId")
                stop_last_updated = entry.get("LastUpdated")

            for rd in route_dirs:
                for dep in (rd.get("Departures") or []):
                    envelope = {
                        "ingested_at": ingested_at,
                        "producer_version": VERSION,
                        "stop_id": stop_id,
                        "stop_record_id": stop_record_id,
                        "stop_last_updated": stop_last_updated,
                        "route_id": rd.get("RouteId"),
                        "route_record_id": rd.get("RouteRecordId"),
                        "direction": rd.get("Direction"),
                        "direction_code": rd.get("DirectionCode"),
                        "is_done": rd.get("IsDone"),
                        "payload": dep,
                    }
                    producer.produce(
                        DEPARTURE_TOPIC,
                        key=str(stop_id).encode(),
                        value=json.dumps(envelope).encode(),
                    )
                    count += 1

    producer.flush()
    return count


if __name__ == "__main__":
    print(f"producer {VERSION} starting: vehicles/{VEHICLE_EVERY}s, "
          f"departures/{DEPARTURE_EVERY}s", flush=True)

    start = time.time()
    last_dep = -DEPARTURE_EVERY   # fire departures on the first pass

    while True:
        loop_started = time.time()
        elapsed = loop_started - start

        n = poll_vehicles()
        print(f"[{datetime.now():%H:%M:%S}] vehicles: {n}", flush=True)

        if elapsed - last_dep >= DEPARTURE_EVERY:
            d = poll_departures()
            last_dep = elapsed
            print(f"[{datetime.now():%H:%M:%S}] departures: {d}", flush=True)

        time.sleep(max(0, VEHICLE_EVERY - (time.time() - loop_started)))