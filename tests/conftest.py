"""Shared fixtures: an in-process Kafka broker and a consumer run as a real
subprocess, so signals, crashes and restarts behave as they do under systemd.

The broker is librdkafka's built-in mock cluster (test.mock.num.brokers):
real Kafka protocol, consumer groups and offset commits, no Docker needed.
"""
import json
import os
import pathlib
import signal
import subprocess
import sys
import time

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

REPO = pathlib.Path(__file__).resolve().parents[1]
FAKES = REPO / "tests" / "fakes"
CONSUMER = os.getenv("CONSUMER_UNDER_TEST", str(REPO / "consumer" / "consume.py"))
VT, DT = "tcat.vehicle_positions.v1", "tcat.stop_departures.v1"


def wait_for(cond, timeout=30.0, every=0.25):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(every)
    return False


class Pipeline:
    """One mock cluster, one fake bucket, helpers to drive the consumer."""

    def __init__(self, tmp: pathlib.Path):
        self.tmp = tmp
        self.gcs = tmp / "gcs"
        self.gcs.mkdir()
        self.cluster = Producer({"test.mock.num.brokers": 1, "log_level": 3})   # keeps the cluster alive
        broker = next(iter(self.cluster.list_topics(timeout=10).brokers.values()))
        self.bootstrap = f"{broker.host}:{broker.port}"
        self.producer = Producer({"bootstrap.servers": self.bootstrap, "log_level": 3})
        self.seq = 0
        self.procs: list[subprocess.Popen] = []

    # -- producing ---------------------------------------------------------
    def send(self, topic: str, n: int = 1, payload=None) -> list[int]:
        sent = []
        for _ in range(n):
            self.seq += 1
            env = {"seq": self.seq, "ingested_at": "2026-11-03T15:00:00+00:00",
                   "payload": payload if payload is not None else {"VehicleId": self.seq}}
            self.producer.produce(topic, key=str(self.seq % 5).encode(),
                                  value=json.dumps(env).encode())
            sent.append(self.seq)
        assert self.producer.flush(10) == 0
        return sent

    def send_raw(self, topic: str, value):
        self.producer.produce(topic, value=value)
        assert self.producer.flush(10) == 0

    # -- the consumer process -----------------------------------------------
    def start(self, **env) -> subprocess.Popen:
        e = dict(os.environ, KAFKA_BROKER=self.bootstrap, GCS_BUCKET="bkt",
                 GROUP_ID="test-sink", FAKE_GCS_DIR=str(self.gcs),
                 PYTHONPATH=str(FAKES), KAFKA_SESSION_TIMEOUT_MS="6000",
                 MAX_SECONDS="2", MAX_RECORDS="500")
        e.update({k: str(v) for k, v in env.items()})
        p = subprocess.Popen([sys.executable, "-u", CONSUMER], env=e, text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.procs.append(p)
        return p

    @staticmethod
    def stop(p: subprocess.Popen, sig=signal.SIGTERM, timeout=20) -> tuple[int, str]:
        if p.poll() is None:
            p.send_signal(sig)
        try:
            out, _ = p.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            out, _ = p.communicate()
        return p.returncode, out

    def fault(self, name: str, count: int = 1):
        (self.gcs / f".fault_{name}").write_text(str(count))

    # -- reading results ----------------------------------------------------
    def lines(self, prefix: str = "raw") -> list[dict]:
        out = []
        for f in sorted((self.gcs / "bkt" / prefix).rglob("*.jsonl")) if (self.gcs / "bkt" / prefix).exists() else []:
            out += [json.loads(l) for l in f.read_text().splitlines() if l]
        return out

    def seqs(self) -> list[int]:
        return sorted(r["seq"] for r in self.lines("raw"))

    def committed(self) -> int:
        """Total committed offset across both topics for the sink's group."""
        c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": "test-sink",
                      "enable.auto.commit": False})
        total = 0
        for t in (VT, DT):
            md = c.list_topics(t, timeout=10).topics[t]
            tps = c.committed([TopicPartition(t, p) for p in md.partitions], timeout=10)
            total += sum(max(tp.offset, 0) for tp in tps)
        c.close()
        return total

    def close(self):
        for p in self.procs:
            if p.poll() is None:
                p.kill()
                p.communicate()


@pytest.fixture
def pipe(tmp_path):
    p = Pipeline(tmp_path)
    yield p
    p.close()
