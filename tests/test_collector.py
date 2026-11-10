"""scripts/collect.sh against a local fake of the TCAT API."""
import gzip
import http.server
import json
import pathlib
import signal
import subprocess
import threading
import time

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
FIX = REPO / "tests" / "fixtures"


class FakeTcat(http.server.ThreadingHTTPServer):
    def __init__(self):
        self.fail_stops = False
        self.hits: list[str] = []
        super().__init__(("127.0.0.1", 0), Handler)


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        srv, path = self.server, self.path
        srv.hits.append(path)
        if path == "/PublicMessages/GetCurrentMessages":
            body = [{"MessageId": 1, "Message": "Detour on State St"}]
        elif path == "/Stops/GetAllStops":
            if srv.fail_stops:
                return self.send_error(500)
            body = [{"StopId": 100, "Name": "Ithaca Commons - Seneca St"}]
        elif path == "/Routes/GetVisibleRoutes":
            body = json.loads((FIX / "visible_routes.json").read_text())
        elif path.startswith("/RouteDetails/Get/"):
            body = {"RouteId": int(path.rsplit("/", 1)[1]), "Stops": []}
        else:
            return self.send_error(404)
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def api():
    srv = FakeTcat()
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv
    srv.shutdown()


def run_collector(api, root, seconds, **env):
    e = {"PATH": "/usr/bin:/bin", "TCAT_BASE": f"http://127.0.0.1:{api.server_port}",
         "ROOT": str(root), "MESSAGE_EVERY": "1", "DIM_RETRY_EVERY": "2",
         "NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"}
    e.update(env)
    p = subprocess.Popen(["bash", str(REPO / "scripts" / "collect.sh")], env=e,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    time.sleep(seconds)
    p.send_signal(signal.SIGTERM)
    out, _ = p.communicate(timeout=10)
    return p.returncode, out


def files(root, pattern):
    return sorted(pathlib.Path(root).rglob(pattern))


def test_collects_alerts_and_a_complete_daily_snapshot(api, tmp_path):
    rc, out = run_collector(api, tmp_path, 4)
    assert rc == 0, out
    msgs = files(tmp_path, "messages_*.json.gz")
    assert len(msgs) >= 2
    assert json.loads(gzip.decompress(msgs[0].read_bytes()))[0]["MessageId"] == 1
    [dims] = files(tmp_path / "dims", "dt=*")
    names = sorted(p.name for p in dims.iterdir())
    # route ids 10, 30, 81 come from the fixture's visible routes
    assert names == [".done", "route_10.json.gz", "route_30.json.gz", "route_81.json.gz", "stops.json.gz"]
    assert sum(h.startswith("/Stops/") for h in api.hits) == 1      # done once, not every loop


def test_never_stores_vehicles_departures_or_driver_names(api, tmp_path):
    run_collector(api, tmp_path, 3)
    assert not (tmp_path / "raw" / "vehicles").exists()
    assert not (tmp_path / "raw" / "departures").exists()
    assert not any(h.startswith("/StopDepartures/") for h in api.hits)
    for f in files(tmp_path, "*.gz"):
        assert b"Driver" not in gzip.decompress(f.read_bytes()), f


def test_failed_snapshot_is_not_marked_done_and_is_retried(api, tmp_path):
    api.fail_stops = True
    run_collector(api, tmp_path, 3)
    assert files(tmp_path, ".done") == []
    meta = [json.loads(l) for f in files(tmp_path, "meta.ndjson") for l in f.read_text().splitlines()]
    assert any(m["source"] == "dim_stops" and m["http_code"] == 500 and m["stored"] is False for m in meta)
    assert all(m["writer"] == "collector" for m in meta)

    api.fail_stops = False                      # endpoint recovers
    run_collector(api, tmp_path, 4)
    assert len(files(tmp_path, ".done")) == 1
    assert files(tmp_path, "stops.json.gz")


def test_no_partial_files_left_behind(api, tmp_path):
    run_collector(api, tmp_path, 3)
    assert files(tmp_path, "*.part") == []
