"""deploy/topics.sh against a fake rpk: topics and retention as code."""
import os
import pathlib
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
TOPICS = ["tcat.vehicle_positions.v1", "tcat.stop_departures.v1"]
CONFIG = "retention.ms=604800000 retention.bytes=536870912"


def run(tmp_path, **env):
    e = dict(os.environ, RPK=f"bash {REPO / 'tests' / 'fakes' / 'rpk'}", FAKE_RPK_DIR=str(tmp_path), **env)
    p = subprocess.run(["bash", str(REPO / "deploy" / "topics.sh")], env=e,
                       capture_output=True, text=True)
    calls = (tmp_path / "calls").read_text().splitlines() if (tmp_path / "calls").exists() else []
    return p.returncode, p.stdout + p.stderr, calls


def test_creates_missing_topics_with_config(tmp_path):
    rc, out, calls = run(tmp_path)
    assert rc == 0, out
    for t in TOPICS:
        assert f"topic create {t} -p 3 -c retention.ms=604800000 -c retention.bytes=536870912" in calls
        assert f"topic alter-config {t} --set retention.ms=604800000 --set retention.bytes=536870912" in calls


def test_existing_topics_get_config_reapplied_not_recreated(tmp_path):
    (tmp_path / "topics").write_text("\n".join(TOPICS) + "\n")
    rc, out, calls = run(tmp_path)
    assert rc == 0, out
    assert not any(c.startswith("topic create") for c in calls)
    assert sum(c.startswith("topic alter-config") for c in calls) == 2


@pytest.mark.parametrize("step", ["list", "create", "alter"])
def test_real_errors_fail_the_script(tmp_path, step):
    # the old setup hid every error behind `>/dev/null 2>&1 || true`
    rc, out, _ = run(tmp_path, FAKE_RPK_FAIL=step)
    assert rc != 0


def test_balancing_settings_off_and_unknown_ones_skipped(tmp_path):
    rc, out, calls = run(tmp_path, FAKE_RPK_UNKNOWN="core_balancing_continuous")
    assert rc == 0, out
    assert "cluster config set partition_autobalancing_mode node_add" in calls
    assert not any("set core_balancing_continuous" in c for c in calls)
    assert "no setting core_balancing_continuous" in out
