"""producer/produce.py: payload handling and delivery accounting."""
import csv
import importlib.util
import json
import pathlib
import tempfile
import time

import pytest
from confluent_kafka import Consumer, Producer

REPO = pathlib.Path(__file__).resolve().parents[1]
FIX = REPO / "tests" / "fixtures"
SEED = REPO / "dbt" / "tcat" / "seeds" / "watched_stops.csv"


OK = {"http_code": 200, "bytes": 1000, "error": None}


def fixture(name):
    return json.loads((FIX / name).read_text())


def ok(name):
    """A fetch() stand-in that returns a fixture as a successful response."""
    return lambda url: (fixture(name), OK)


def request_log(p):
    return [json.loads(l) for f in sorted(p.REQUEST_LOG_DIR.rglob("producer.ndjson"))
            for l in f.read_text().splitlines()]


@pytest.fixture
def mock_bootstrap():
    cluster = Producer({"test.mock.num.brokers": 1, "log_level": 3})
    broker = next(iter(cluster.list_topics(timeout=10).brokers.values()))
    yield f"{broker.host}:{broker.port}"


def load_producer(monkeypatch, broker, timeout_ms=20000, log_dir=None):
    """Import a fresh copy of produce.py against the given broker."""
    monkeypatch.setenv("KAFKA_BROKER", broker)
    monkeypatch.setenv("KAFKA_DELIVERY_TIMEOUT_MS", str(timeout_ms))
    monkeypatch.setenv("REQUEST_LOG_DIR", log_dir or tempfile.mkdtemp())
    spec = importlib.util.spec_from_file_location(f"produce_{time.time_ns()}",
                                                  REPO / "producer" / "produce.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def read_topic(bootstrap, topic, n, timeout=20):
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "reader",
                  "auto.offset.reset": "earliest", "log_level": 3})
    c.subscribe([topic])
    out, end = [], time.time() + timeout
    while len(out) < n and time.time() < end:
        m = c.poll(0.5)
        if m is not None and not m.error():
            out.append(m)
    c.close()
    return out


def test_vehicles_are_published_keyed_and_untouched(monkeypatch, mock_bootstrap):
    p = load_producer(monkeypatch, mock_bootstrap)
    monkeypatch.setattr(p, "fetch", ok("visible_routes.json"))
    stats = p.poll_vehicles()
    # 3 good vehicles; skipped: one without VehicleId, one non-object route
    assert (stats.produced, stats.delivered, stats.failed, stats.skipped) == (3, 3, 0, 2)
    msgs = read_topic(mock_bootstrap, p.VEHICLE_TOPIC, 3)
    assert sorted(m.key() for m in msgs) == [b"1701", b"1702", b"1830"]
    first = next(json.loads(m.value()) for m in msgs if m.key() == b"1701")
    assert first["route_id"] == 10 and first["producer_version"] == p.VERSION
    assert first["payload"] == fixture("visible_routes.json")[0]["Vehicles"][0]   # raw stays raw


def test_departures_wrapper_and_bare_shapes(monkeypatch, mock_bootstrap):
    p = load_producer(monkeypatch, mock_bootstrap)
    wrapper = list(p.departure_envelopes(1541, fixture("departures_wrapper.json"), "t"))
    assert len(wrapper) == 2                                   # Departures: null -> none
    assert {e["stop_record_id"] for e in wrapper} == {77}
    assert [e["payload"]["Trip"]["TripId"] for e in wrapper] == [1900, 1930]
    bare = list(p.departure_envelopes(1541, fixture("departures_bare.json"), "t"))
    assert len(bare) == 1 and bare[0]["stop_record_id"] is None and bare[0]["route_id"] == 11


@pytest.mark.parametrize("body, skipped", [
    ({"Message": "An error has occurred."}, 1),               # object, not a list
    (["oops"], 1),                                             # entry not an object
    ([{"RouteDirections": ["oops"]}], 1),                      # direction not an object
    ([{"RouteDirections": [{"Departures": ["oops", {}]}]}], 1),  # one bad, one ok
    ([], 0),
])
def test_unexpected_departure_shapes_are_skipped_not_fatal(monkeypatch, mock_bootstrap, body, skipped):
    p = load_producer(monkeypatch, mock_bootstrap)
    out = list(p.departure_envelopes(100, body, "t"))
    assert out.count(None) == skipped


def test_api_failure_is_counted(monkeypatch, mock_bootstrap):
    p = load_producer(monkeypatch, mock_bootstrap)
    monkeypatch.setattr(p, "fetch", lambda url: (None, {"http_code": 503, "bytes": 0, "error": "503 Server Error"}))
    stats = p.poll_departures()
    assert stats.api_errors == len(p.DEPARTURE_STOPS) and stats.produced == 0


def test_dead_broker_failures_are_counted_and_poll_returns(monkeypatch):
    # Nothing listens on port 1: Redpanda is down. The old producer returned
    # "vehicles: 25" here and blocked for up to 5 minutes.
    p = load_producer(monkeypatch, "127.0.0.1:1", timeout_ms=2000)
    monkeypatch.setattr(p, "fetch", ok("visible_routes.json"))
    t0 = time.time()
    stats = p.poll_vehicles()
    assert time.time() - t0 < 10
    assert (stats.produced, stats.delivered, stats.failed) == (3, 0, 3)
    assert stats.last_error is not None


def test_stop_list_is_the_seed(monkeypatch, mock_bootstrap):
    p = load_producer(monkeypatch, mock_bootstrap)
    with open(SEED, newline="") as f:
        seed = [int(r["stop_id"]) for r in csv.DictReader(f)]
    assert p.DEPARTURE_STOPS == seed
    assert len(seed) == 21 and len(set(seed)) == 21


def test_messages_are_zstd_compressed_and_round_trip(monkeypatch, mock_bootstrap):
    p = load_producer(monkeypatch, mock_bootstrap)
    assert p.producer is not None
    monkeypatch.setattr(p, "fetch", ok("visible_routes.json"))
    assert p.poll_vehicles().delivered == 3
    msgs = read_topic(mock_bootstrap, p.VEHICLE_TOPIC, 3)
    assert len(msgs) == 3                                     # consumer decompressed them
    assert all(json.loads(m.value())["producer_version"] == p.VERSION for m in msgs)


# -- request log (D11) ------------------------------------------------------

def test_request_log_has_one_line_per_vehicle_poll_with_final_counts(monkeypatch, mock_bootstrap):
    p = load_producer(monkeypatch, mock_bootstrap)
    monkeypatch.setattr(p, "fetch", ok("visible_routes.json"))
    p.poll_vehicles()
    [line] = request_log(p)
    assert line["writer"] == "producer" and line["source"] == "vehicles"
    assert (line["http_code"], line["records"], line["produced"], line["delivered"],
            line["failed"], line["skipped"]) == (200, 4, 3, 3, 0, 2)
    assert line["envelope_ingested_at"] is not None and line["error"] is None


def test_request_log_has_one_line_per_stop(monkeypatch, mock_bootstrap):
    p = load_producer(monkeypatch, mock_bootstrap)
    monkeypatch.setattr(p, "fetch", ok("departures_wrapper.json"))
    stats = p.poll_departures()
    lines = request_log(p)
    assert sorted(l["stop_id"] for l in lines) == sorted(p.DEPARTURE_STOPS)
    assert all(l["delivered"] == 2 and l["records"] == 2 for l in lines)
    assert stats.delivered == 2 * len(p.DEPARTURE_STOPS)


def test_api_failure_is_logged(monkeypatch, mock_bootstrap):
    p = load_producer(monkeypatch, mock_bootstrap)
    monkeypatch.setattr(p, "fetch", lambda url: (None, {"http_code": 503, "bytes": 0, "error": "503 Server Error"}))
    p.poll_vehicles()
    [line] = request_log(p)
    assert line["http_code"] == 503 and "503" in line["error"]
    assert line["envelope_ingested_at"] is None and line["produced"] == 0


def test_request_log_still_written_while_broker_is_down(monkeypatch):
    # the point of a local log: it records the polls that Kafka lost
    p = load_producer(monkeypatch, "127.0.0.1:1", timeout_ms=2000)
    monkeypatch.setattr(p, "fetch", ok("visible_routes.json"))
    p.poll_vehicles()
    [line] = request_log(p)
    assert (line["produced"], line["delivered"], line["failed"]) == (3, 0, 3)
    assert line["error"].startswith("delivery")


def test_request_log_failure_does_not_stop_collection(monkeypatch, mock_bootstrap, tmp_path, capsys):
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("")                       # the log dir can't be created under a file
    p = load_producer(monkeypatch, mock_bootstrap, log_dir=str(blocker))
    monkeypatch.setattr(p, "fetch", ok("visible_routes.json"))
    assert p.poll_vehicles().delivered == 3
    assert "request log not written" in capsys.readouterr().err
