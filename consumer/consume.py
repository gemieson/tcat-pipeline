"""Read TCAT events off Kafka and write newline-delimited JSON to GCS.

Subscribes to both topics and keeps one buffer per topic, but always flushes
them TOGETHER: when any buffer is full or the oldest unflushed record is old
enough, every non-empty buffer is uploaded, and only then are offsets
committed -- once.

Why together: consumer.commit() with no arguments commits the position of
every assigned partition, in both topics. Committing after uploading just one
topic's buffer would mark the other topic's still-in-memory records as done,
and a crash would lose them. With a single flush for everything, "committed"
always means "already in GCS".

Guarantee: at-least-once from Kafka to GCS. A crash after an upload but before
the commit re-delivers that batch, so exact copies can land in GCS. Staging
removes them (see dbt/tcat/models/staging).
"""
from __future__ import annotations   # type hints work on Python < 3.10

import base64, json, os, signal, sys, time, uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
BUCKET = os.environ["GCS_BUCKET"]          # required, no default -- fail loudly
GROUP = os.getenv("GROUP_ID", "tcat-gcs-sink")

VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "tcat.vehicle_positions.v1")
DEPARTURE_TOPIC = os.getenv("DEPARTURE_TOPIC", "tcat.stop_departures.v1")

# buffer key -> GCS prefix. QUARANTINE holds messages that aren't a JSON
# object; they are kept (base64) for inspection, outside the raw/ tables.
QUARANTINE = "quarantine"
PREFIX = {
    VEHICLE_TOPIC: "raw/vehicles",
    DEPARTURE_TOPIC: "raw/departures",
    QUARANTINE: "quarantine",
}
TOPICS = [VEHICLE_TOPIC, DEPARTURE_TOPIC]

MAX_RECORDS = int(os.getenv("MAX_RECORDS", "500"))    # flush when a buffer has this many
MAX_SECONDS = int(os.getenv("MAX_SECONDS", "120"))    # ...or the oldest record is this old

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
    # Manual commit is what makes this crash-safe: offsets move only after
    # the records they cover are in GCS.
    "enable.auto.commit": False,
    # How long the broker waits for a dead consumer before handing its
    # partitions to a new one (e.g. after a hard kill and restart).
    "session.timeout.ms": int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "45000")),
})

bucket = storage.Client().bucket(BUCKET)

buffers: dict[str, list[str]] = {k: [] for k in PREFIX}
batch_started: float | None = None   # when the oldest unflushed record arrived
running = True
crashed = False   # set on the error path: from then on, never commit


def _stop(signum, frame):
    global running
    running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def log(text: str, err: bool = False) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {text}",
          file=sys.stderr if err else sys.stdout, flush=True)


def prepare(msg) -> tuple[str, str]:
    """Return (buffer key, NDJSON line) for one Kafka message.

    A message that isn't a UTF-8 JSON object goes to quarantine with its
    topic/partition/offset, instead of crashing the consumer on every restart.
    """
    raw = msg.value()
    try:
        text = raw.decode("utf-8")            # AttributeError if value is None
        obj = json.loads(text)                # ValueError if not JSON
        if not isinstance(obj, dict):
            raise ValueError(f"expected a JSON object, got {type(obj).__name__}")
    except (AttributeError, UnicodeDecodeError, ValueError) as e:
        return QUARANTINE, json.dumps({
            "topic": msg.topic(), "partition": msg.partition(),
            "offset": msg.offset(), "error": str(e),
            "value_b64": base64.b64encode(raw).decode() if raw is not None else None,
        })

    if STRIP_DRIVER and isinstance(obj.get("payload"), dict):
        for f in DRIVER_FIELDS:
            obj["payload"].pop(f, None)
        text = json.dumps(obj)
    elif "\n" in text:                        # one record per line, always
        text = json.dumps(obj)
    return msg.topic(), text


def upload(key: str) -> None:
    """Write one buffer to a new GCS object.

    The client retries failed uploads itself (by default in
    google-cloud-storage 3.x; in 2.x only when a precondition is set, as
    here). if_generation_match=0 means "create only, never overwrite", so a
    retry can't replace anything. If an earlier attempt succeeded but its
    response was lost, the retry gets 412 PreconditionFailed. The random
    suffix makes the name unique to this call, so an object already at this
    path can only be this batch: treat it as written.
    """
    now = datetime.now(timezone.utc)
    path = (f"{PREFIX[key]}/dt={now:%Y-%m-%d}/hr={now:%H}/"
            f"{now:%Y%m%dT%H%M%S_%fZ}_{uuid.uuid4().hex[:8]}.jsonl")
    try:
        bucket.blob(path).upload_from_string(
            "\n".join(buffers[key]), content_type="application/x-ndjson",
            if_generation_match=0,
        )
    except PreconditionFailed:
        log(f"{path} already exists: an earlier attempt of this upload succeeded")
    log(f"wrote {len(buffers[key]):>4} -> gs://{BUCKET}/{path}")


def flush_all() -> None:
    """Upload every non-empty buffer, then commit once.

    If an upload raises, nothing is committed and the exception propagates:
    systemd restarts the process and Kafka re-delivers everything since the
    last commit. Buffers already uploaded are cleared first, so no code path
    uploads them a second time.
    """
    global batch_started
    for key in [k for k, recs in buffers.items() if recs]:
        upload(key)
        buffers[key] = []
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as e:
        # Nothing consumed since the last commit: there was nothing to commit.
        if e.args[0].code() != KafkaError._NO_OFFSET:
            raise
    batch_started = None


def _on_revoke(consumer_, partitions) -> None:
    """Before partitions move to another consumer (a second instance started,
    or this one is closing), write and commit what we hold for them.

    Not after a crash: close() also lands here, and a commit then would mark
    records that never reached GCS as done.
    """
    if not crashed:
        flush_all()


def main() -> None:
    global batch_started, crashed
    consumer.subscribe(TOPICS, on_revoke=_on_revoke)
    log(f"consumer starting: {', '.join(TOPICS)} -> gs://{BUCKET}/")

    clean_exit = False
    try:
        while running:
            msg = consumer.poll(1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log(f"[warn] {msg.error()}", err=True)
                else:
                    key, line = prepare(msg)
                    if key == QUARANTINE:
                        log(f"[warn] quarantined {msg.topic()}[{msg.partition()}]"
                            f"@{msg.offset()}", err=True)
                    buffers[key].append(line)
                    if batch_started is None:
                        batch_started = time.time()

            full = any(len(b) >= MAX_RECORDS for b in buffers.values())
            aged = batch_started is not None and time.time() - batch_started >= MAX_SECONDS
            if full or aged:
                flush_all()
        clean_exit = True
    finally:
        if clean_exit:
            # Graceful stop (SIGTERM): don't make Kafka re-deliver what we hold.
            flush_all()
        else:
            # After a crash, don't retry or commit: anything not committed is
            # re-delivered on restart, so nothing is lost by stopping now.
            crashed = True
        consumer.close()
        log("consumer stopped")


if __name__ == "__main__":
    main()
