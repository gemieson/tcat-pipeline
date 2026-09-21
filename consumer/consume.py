"""Read TCAT events off Kafka and write newline-delimited JSON to GCS.

Subscribes to both topics, buffers each separately, and flushes to a
date/hour-partitioned path per stream. Offsets are committed ONLY after a
confirmed upload, so a crash produces duplicates (dedupable in dbt) rather
than gaps (unrecoverable).
"""
import json, os, signal, sys, time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from google.cloud import storage

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
BUCKET = os.environ["GCS_BUCKET"]          # required, no default -- fail loudly
GROUP = os.getenv("GROUP_ID", "tcat-gcs-sink")

VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "tcat.vehicle_positions.v1")
DEPARTURE_TOPIC = os.getenv("DEPARTURE_TOPIC", "tcat.stop_departures.v1")

# topic -> GCS prefix
PREFIX = {
    VEHICLE_TOPIC: "raw/vehicles",
    DEPARTURE_TOPIC: "raw/departures",
}

MAX_RECORDS = int(os.getenv("MAX_RECORDS", "500"))    # flush at this many
MAX_SECONDS = int(os.getenv("MAX_SECONDS", "120"))    # ...or this old

# TCAT embeds driver names in the vehicle payload. Set STRIP_DRIVER=1 to drop
# them before they ever reach the cloud. Off by default so "raw" stays raw --
# turn it on if this project will be public.
STRIP_DRIVER = os.getenv("STRIP_DRIVER") == "1"
DRIVER_FIELDS = ("DriverName", "DriverFirstName", "DriverLastName",
                 "DriverFareboxId")

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP,
    "auto.offset.reset": "earliest",
    # Manual commit is what makes this crash-safe. With auto-commit,
    # dying between "read" and "write to GCS" marks messages consumed
    # that never landed -- silent data loss. Committing only after a
    # confirmed write means a crash causes DUPLICATES, not GAPS.
    # Duplicates you dedup in dbt. Gaps are unrecoverable.
    "enable.auto.commit": False,
})

bucket = storage.Client().bucket(BUCKET)

# one buffer per topic, so a quiet stream never holds up a busy one
buffers: dict[str, list[str]] = {t: [] for t in PREFIX}
started: dict[str, float] = {t: time.time() for t in PREFIX}
running = True


def _stop(signum, frame):
    global running
    running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def scrub(raw: str) -> str:
    if not STRIP_DRIVER:
        return raw
    obj = json.loads(raw)
    payload = obj.get("payload")
    if isinstance(payload, dict):
        for f in DRIVER_FIELDS:
            payload.pop(f, None)
    return json.dumps(obj)


def flush(topic: str) -> None:
    """Upload one topic's buffer, then commit. Order matters -- never the
    reverse. The commit covers every topic read so far, which is why we only
    call it after a successful upload."""
    records = buffers[topic]
    if not records:
        return

    now = datetime.now(timezone.utc)
    path = (f"{PREFIX[topic]}/dt={now:%Y-%m-%d}/hr={now:%H}/"
            f"{now:%Y%m%dT%H%M%S_%fZ}.jsonl")   # microseconds: two flushes in one second must not collide

    bucket.blob(path).upload_from_string(
        "\n".join(records), content_type="application/x-ndjson"
    )
    consumer.commit(asynchronous=False)   # only after the upload succeeded

    print(f"[{now:%H:%M:%S}] wrote {len(records):>4} -> gs://{BUCKET}/{path}",
          flush=True)
    buffers[topic] = []
    started[topic] = time.time()


def flush_all() -> None:
    for t in PREFIX:
        try:
            flush(t)
        except Exception as e:
            print(f"[error] flush {t} failed: {e}", file=sys.stderr, flush=True)


def main() -> None:
    consumer.subscribe(list(PREFIX))
    print(f"consumer starting: {', '.join(PREFIX)} -> gs://{BUCKET}/",
          flush=True)

    try:
        while running:
            msg = consumer.poll(1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[warn] {msg.error()}", file=sys.stderr,
                              flush=True)
                else:
                    topic = msg.topic()
                    if topic in buffers:
                        buffers[topic].append(scrub(msg.value().decode()))

            now = time.time()
            for t in PREFIX:
                aged = now - started[t] >= MAX_SECONDS
                if len(buffers[t]) >= MAX_RECORDS or (buffers[t] and aged):
                    flush(t)
    finally:
        # Don't lose what's in memory on shutdown.
        flush_all()
        consumer.close()
        print("consumer stopped", flush=True)


if __name__ == "__main__":
    main()