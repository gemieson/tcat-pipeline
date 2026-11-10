"""Delivery guarantees of consumer/consume.py, end to end.

Each test runs the real consumer as a subprocess against a mock Kafka broker
and a fake bucket. "Exactly once" below means each produced record appears in
storage once; "at least once" means none is missing.
"""
import collections
import signal
import time

from conftest import DT, VT, wait_for


def test_both_buffers_due_together_writes_each_record_once(pipe):
    # Both buffers age out in the same loop iteration. The old consumer
    # crashed here (_NO_OFFSET on the second commit) and re-uploaded a batch.
    sent = pipe.send(VT, 10) + pipe.send(DT, 10)
    c = pipe.start(MAX_SECONDS=2)
    assert wait_for(lambda: len(pipe.seqs()) >= len(sent))
    time.sleep(4)                                   # give a crash time to happen
    assert c.poll() is None, "consumer exited: " + pipe.stop(c)[1][-600:]
    rc, out = pipe.stop(c)
    assert rc == 0, out[-600:]
    assert pipe.seqs() == sorted(sent)              # exactly once
    assert pipe.committed() == len(sent)


def test_hard_kill_loses_nothing(pipe):
    # Departures are still in memory when vehicles fill their buffer. The old
    # consumer's vehicle flush committed the departures' offsets too, so a hard
    # kill (OOM, reboot) lost them for good.
    sent = pipe.send(DT, 3) + pipe.send(VT, 5)
    c = pipe.start(MAX_RECORDS=5, MAX_SECONDS=600)
    assert wait_for(lambda: pipe.seqs())
    sent += pipe.send(DT, 2)                        # buffered, below both limits
    time.sleep(3)
    pipe.stop(c, signal.SIGKILL)

    c2 = pipe.start(MAX_SECONDS=2)                  # what systemd does next
    assert wait_for(lambda: set(sent) <= set(pipe.seqs()), timeout=40), \
        f"missing {sorted(set(sent) - set(pipe.seqs()))}"
    rc, out = pipe.stop(c2)
    assert rc == 0, out[-600:]


def test_storage_outage_commits_nothing_then_recovers(pipe):
    pipe.fault("fail", 1)                           # GCS down for the first flush
    sent = pipe.send(VT, 4) + pipe.send(DT, 4)
    c = pipe.start(MAX_SECONDS=2)
    assert wait_for(lambda: c.poll() is not None)   # crashes; systemd restarts it
    assert c.returncode != 0
    # the invariant: nothing is marked done that isn't stored
    assert pipe.committed() <= len(set(pipe.seqs()))

    c2 = pipe.start(MAX_SECONDS=2)                  # GCS is back
    assert wait_for(lambda: len(pipe.seqs()) >= len(sent), timeout=40)
    pipe.stop(c2)
    assert pipe.seqs() == sorted(sent)              # no loss, and here no duplicates


def test_lost_upload_response_is_not_a_duplicate(pipe):
    # The object was stored but the response never arrived; the client's retry
    # of a create-only upload then reports "already exists".
    pipe.fault("lose_response", 1)
    sent = pipe.send(VT, 6)
    c = pipe.start(MAX_SECONDS=2)
    assert wait_for(lambda: pipe.committed() == len(sent))
    assert c.poll() is None
    pipe.stop(c)
    assert pipe.seqs() == sorted(sent)


def test_poison_messages_are_quarantined_not_fatal(pipe):
    good = pipe.send(VT, 3)
    pipe.send_raw(VT, b"not json")
    pipe.send_raw(VT, b"[1, 2, 3]")                 # JSON, but not an object
    pipe.send_raw(VT, b"\xff\xfe")                  # not UTF-8
    pipe.send_raw(VT, None)                         # tombstone
    good += pipe.send(VT, 2)
    c = pipe.start(MAX_SECONDS=2)
    assert wait_for(lambda: len(pipe.seqs()) == len(good) and len(pipe.lines("quarantine")) == 4)
    assert c.poll() is None
    pipe.stop(c)
    q = pipe.lines("quarantine")
    assert {r["topic"] for r in q} == {VT}
    assert all({"partition", "offset", "error", "value_b64"} <= set(r) for r in q)
    assert pipe.committed() == len(good) + 4


def test_raw_mode_keeps_bytes(pipe):
    # STRIP_DRIVER off: the line in storage is the Kafka message, unchanged.
    pipe.send(VT, 1, payload={"VehicleId": 7, "DriverName": "Jane Doe"})
    c = pipe.start(MAX_SECONDS=1)
    assert wait_for(lambda: pipe.seqs())
    pipe.stop(c)
    [f] = list((pipe.gcs / "bkt" / "raw").rglob("*.jsonl"))
    assert '"DriverName": "Jane Doe"' in f.read_text()


def test_strip_driver_removes_names(pipe):
    payload = {"VehicleId": 7, "DriverName": "Jane Doe", "DriverFareboxId": 12}
    pipe.send(VT, 1, payload=payload)
    c = pipe.start(MAX_SECONDS=1, STRIP_DRIVER=1)
    assert wait_for(lambda: pipe.seqs())
    pipe.stop(c)
    [row] = pipe.lines()
    assert row["payload"] == {"VehicleId": 7}


def test_graceful_stop_flushes_and_commits(pipe):
    sent = pipe.send(VT, 5) + pipe.send(DT, 5)
    c = pipe.start(MAX_SECONDS=600)                 # would never flush on its own
    time.sleep(5)                                   # consumed, still in memory
    assert pipe.seqs() == []
    rc, out = pipe.stop(c, signal.SIGTERM)
    assert rc == 0, out[-600:]
    assert pipe.seqs() == sorted(sent)
    assert pipe.committed() == len(sent)


def test_second_instance_rebalance_loses_nothing(pipe):
    # Two consumers in the group, e.g. a manual run while systemd's is up.
    sent = pipe.send(VT, 20) + pipe.send(DT, 20)
    a = pipe.start(MAX_SECONDS=600)
    time.sleep(5)                                   # a holds everything in memory
    b = pipe.start(MAX_SECONDS=2)                   # rebalance: a must flush first
    sent += pipe.send(VT, 10) + pipe.send(DT, 10)
    assert wait_for(lambda: set(sent) <= set(pipe.seqs()), timeout=60), \
        f"missing {sorted(set(sent) - set(pipe.seqs()))}"
    pipe.stop(a)
    pipe.stop(b)
    copies = collections.Counter(pipe.lines() and [r["seq"] for r in pipe.lines()])
    assert max(copies.values()) <= 2                # at-least-once; replays allowed
