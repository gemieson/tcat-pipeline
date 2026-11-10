"""Poll the TCAT Avail feed and publish one message per event.

Two streams, two cadences, one process:
  vehicles   every 30s  -> tcat.vehicle_positions.v1   (keyed by VehicleId)
  departures every 60s  -> tcat.stop_departures.v1     (keyed by StopId)

Payloads are published untouched. All cleaning happens in dbt, where it is
version-controlled, tested and re-runnable.

Delivery: produce() is asynchronous, so success is only known from the
delivery report. Every poll waits for its reports, then writes one request-log
line per API call with how many records it received, produced, delivered,
failed and skipped. Polling the API is best-effort by nature (a missed poll
can't be re-polled), so the goal is that loss is counted and visible, never
silent. mart_pipeline_health reconciles these counts with BigQuery.
"""
from __future__ import annotations   # type hints work on Python < 3.10

import csv, json, os, sys, time
from datetime import datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import KafkaException, Producer

BASE = os.getenv("TCAT_BASE", "https://realtimetcatbus.availtec.com/InfoPoint/rest")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "tcat.vehicle_positions.v1")
DEPARTURE_TOPIC = os.getenv("DEPARTURE_TOPIC", "tcat.stop_departures.v1")

VEHICLE_EVERY = int(os.getenv("VEHICLE_SECONDS", "30"))
DEPARTURE_EVERY = int(os.getenv("DEPARTURE_SECONDS", "60"))
# Give up on a message after this long instead of librdkafka's 5-minute
# default, so a dead broker can't stall polling for several cycles.
DELIVERY_TIMEOUT_MS = int(os.getenv("KAFKA_DELIVERY_TIMEOUT_MS", "20000"))
VERSION = "1.4.0"

# Request log (docs/DECISIONS.md, D11): one JSON line per API call, written to
# local disk beside the side collector's log and synced to GCS hourly by
# tcat-sync. Local on purpose: it still records polls while Redpanda is down.
REQUEST_LOG_DIR = Path(os.getenv("REQUEST_LOG_DIR", "data"))

# The stops in scope live in ONE place: the dbt seed that also names them for
# the GPS match. Adding a stop there adds it here.
STOPS_FILE = Path(os.getenv(
    "STOPS_FILE",
    Path(__file__).resolve().parents[1] / "dbt" / "tcat" / "seeds" / "watched_stops.csv"))


def load_stops(path: Path) -> list[int]:
    with open(path, newline="") as f:
        stops = [int(row["stop_id"]) for row in csv.DictReader(f)]
    if not stops:
        raise SystemExit(f"no stops in {path}")
    return stops


DEPARTURE_STOPS = load_stops(STOPS_FILE)

producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BROKER", "localhost:9092"),
    "linger.ms": 100,
    # The broker drops duplicates caused by the client's own retries.
    "enable.idempotence": True,
    # JSON compresses well: less Redpanda disk and network. The consumer
    # decompresses transparently; the files in GCS are unchanged.
    "compression.type": "zstd",
    "message.timeout.ms": DELIVERY_TIMEOUT_MS,
})


class Request:
    """One API call and everything that happened to its records."""

    def __init__(self, source: str, url: str, stop_id: int | None = None):
        self.source, self.url, self.stop_id = source, url, stop_id
        self.requested_at = datetime.now(timezone.utc).isoformat()
        self.envelope_ingested_at = None       # the ingested_at its envelopes carry
        self.http_code = self.bytes = 0
        self.duration_s = 0.0
        self.error = None
        self.records = self.produced = self.delivered = self.failed = self.skipped = 0

    def fetch(self):
        t0 = time.monotonic()
        body, info = fetch(self.url)
        self.duration_s = round(time.monotonic() - t0, 3)
        self.http_code, self.bytes, self.error = info["http_code"], info["bytes"], info["error"]
        if body is not None:
            self.envelope_ingested_at = datetime.now(timezone.utc).isoformat()
        return body

    def on_delivery(self, err, msg):
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            self.error = self.error or f"delivery: {err}"

    def log_line(self) -> str:
        return json.dumps({
            "ingest_ts": self.requested_at, "writer": "producer",
            "producer_version": VERSION, "source": self.source, "url": self.url,
            "stop_id": self.stop_id, "http_code": self.http_code,
            "duration_s": self.duration_s, "bytes": self.bytes, "error": self.error,
            "records": self.records, "produced": self.produced,
            "delivered": self.delivered, "failed": self.failed, "skipped": self.skipped,
            "envelope_ingested_at": self.envelope_ingested_at,
        })


class PollStats:
    """Totals over one poll's requests, for the journal line."""

    def __init__(self, requests: list[Request]):
        self.requests = requests
        for f in ("produced", "delivered", "failed", "skipped"):
            setattr(self, f, sum(getattr(r, f) for r in requests))
        self.api_errors = sum(r.http_code != 200 or r.envelope_ingested_at is None
                              for r in requests)
        self.last_error = next((r.error for r in reversed(requests) if r.error), None)

    def line(self, name: str) -> str:
        return (f"{name}: produced={self.produced} delivered={self.delivered} "
                f"failed={self.failed} skipped={self.skipped} api_errors={self.api_errors}")


def fetch(url: str):
    """GET and parse JSON. Returns (body or None, info). A transit API will
    time out; log and keep going -- that is the difference between a script
    and a service."""
    info = {"http_code": 0, "bytes": 0, "error": None}
    try:
        resp = requests.get(url, timeout=15)
        info["http_code"], info["bytes"] = resp.status_code, len(resp.content)
        resp.raise_for_status()
        return resp.json(), info
    except (requests.RequestException, ValueError) as e:
        info["error"] = str(e)[:300]
        print(f"[warn] {url} failed: {e}", flush=True)
        return None, info


def publish(req: Request, topic: str, key, envelope: dict) -> None:
    try:
        producer.produce(topic, key=str(key).encode(),
                         value=json.dumps(envelope).encode(),
                         on_delivery=req.on_delivery)
        req.produced += 1
    except (BufferError, KafkaException) as e:   # local queue full / fatal client error
        req.failed += 1
        req.error = req.error or f"produce: {e}"
    producer.poll(0)                              # serve delivery reports as we go


def write_request_log(reqs: list[Request]) -> None:
    """Append one line per request. A failure here (disk full, permissions)
    is reported but doesn't stop collection: the request log is the audit
    trail, Kafka is the data path."""
    for r in reqs:
        path = REQUEST_LOG_DIR / "raw" / "_meta" / f"dt={r.requested_at[:10]}" / "producer.ndjson"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a") as f:
                f.write(r.log_line() + "\n")      # one write per line, so lines never interleave
        except OSError as e:
            print(f"[warn] request log not written ({path}): {e}", file=sys.stderr, flush=True)
            return


def finish(reqs: list[Request]) -> PollStats:
    """Wait for every message of this poll to be delivered or time out, then
    log each request with its final counts."""
    left = producer.flush(DELIVERY_TIMEOUT_MS / 1000 + 5)
    if left and reqs:   # should not happen: each message times out on its own first
        reqs[-1].failed += left
    write_request_log(reqs)
    stats = PollStats(reqs)
    if stats.failed:
        print(f"[warn] {stats.failed} message(s) not delivered: {stats.last_error}",
              file=sys.stderr, flush=True)
    return stats


def poll_vehicles() -> PollStats:
    req = Request("vehicles", f"{BASE}/Routes/GetVisibleRoutes")
    routes = req.fetch()
    if routes is not None and not isinstance(routes, list):
        req.error = f"expected a list, got {type(routes).__name__}"
        req.envelope_ingested_at = None
        routes = None

    for route in routes or []:
        if not isinstance(route, dict):
            req.skipped += 1
            continue
        for vehicle in (route.get("Vehicles") or []):
            req.records += 1
            if not isinstance(vehicle, dict) or vehicle.get("VehicleId") is None:
                req.skipped += 1
                continue
            publish(req, VEHICLE_TOPIC, vehicle["VehicleId"], {
                "ingested_at": req.envelope_ingested_at,
                "producer_version": VERSION,
                "route_id": route.get("RouteId"),
                "route_record_id": route.get("RouteRecordId"),
                "payload": vehicle,
            })
    return finish([req])


def departure_envelopes(stop_id: int, body, ingested_at: str):
    """Yield one envelope per departure in a StopDepartures response.

    Avail wraps the payload: a one-element list holding the stop, with
    RouteDirections nested inside, and Departures inside those. Older/other
    deployments return the bare RouteDirections array, so handle both.
    Anything else is skipped (yields None) rather than crashing the poll.
    """
    if not isinstance(body, list):
        yield None
        return
    for entry in body:
        if not isinstance(entry, dict):
            yield None
            continue
        # wrapper shape -> pull the nested list; bare shape -> use as-is
        route_dirs = entry.get("RouteDirections")
        if route_dirs is None:
            route_dirs = [entry]
            stop_record_id = stop_last_updated = None
        else:
            stop_record_id = entry.get("StopRecordId")
            stop_last_updated = entry.get("LastUpdated")

        for rd in route_dirs:
            if not isinstance(rd, dict):
                yield None
                continue
            for dep in (rd.get("Departures") or []):
                if not isinstance(dep, dict):
                    yield None
                    continue
                yield {
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


def poll_departures() -> PollStats:
    """One call per stop. ingested_at is stamped per stop, right after its
    fetch, so it records when that board was actually read."""
    reqs = []
    for stop_id in DEPARTURE_STOPS:
        req = Request("departures", f"{BASE}/StopDepartures/Get/{stop_id}", stop_id)
        reqs.append(req)
        body = req.fetch()
        if body is None:
            continue
        for envelope in departure_envelopes(stop_id, body, req.envelope_ingested_at):
            req.records += 1
            if envelope is None:
                req.skipped += 1
            else:
                publish(req, DEPARTURE_TOPIC, stop_id, envelope)
    return finish(reqs)


if __name__ == "__main__":
    print(f"producer {VERSION} starting: vehicles/{VEHICLE_EVERY}s, "
          f"{len(DEPARTURE_STOPS)} stops/{DEPARTURE_EVERY}s, "
          f"request log under {REQUEST_LOG_DIR}/raw/_meta/", flush=True)

    start = time.time()
    last_dep = -DEPARTURE_EVERY   # fire departures on the first pass

    while True:
        loop_started = time.time()
        elapsed = loop_started - start

        print(f"[{datetime.now():%H:%M:%S}] {poll_vehicles().line('vehicles')}", flush=True)

        if elapsed - last_dep >= DEPARTURE_EVERY:
            stats = poll_departures()
            last_dep = elapsed
            print(f"[{datetime.now():%H:%M:%S}] {stats.line('departures')}", flush=True)

        time.sleep(max(0, VEHICLE_EVERY - (time.time() - loop_started)))
