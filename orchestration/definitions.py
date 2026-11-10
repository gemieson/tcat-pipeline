"""Dagster orchestration for the TCAT pipeline.

What Dagster manages:
  * every dbt model as an asset (raw sources -> staging -> intermediate -> marts),
    so the UI shows the full lineage graph and each model's run history
  * a daily `dbt build` (models + unit and data tests) at 06:20 Eastern, after
    the overnight gap in service. The marts answer "how late is each route by
    hour of day", which an hourly refresh doesn't improve, and every build
    costs BigQuery bytes.
  * an hourly source-freshness check at :45, which fails loudly if the
    producer/consumer stop delivering data
  * a push alert when any run fails (TCAT_ALERT_URL, e.g. an ntfy.sh topic)

What it doesn't manage: the producer, consumer and bash collector. Those are
long-running streaming services and stay under systemd, which restarts them
if they crash. Dagster orchestrates the batch side: transformation and checks.
"""
import os
import sys
import urllib.request
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    OpExecutionContext,
    RetryPolicy,
    RunFailureSensorContext,
    ScheduleDefinition,
    define_asset_job,
    job,
    op,
    run_failure_sensor,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_DIR = Path(__file__).resolve().parent.parent / "dbt" / "tcat"
# profiles.yml lives in ~/.dbt (it points at credentials, so it stays out of git)
PROFILES_DIR = Path.home() / ".dbt"
# use the dbt that sits next to this Python, i.e. the same virtualenv
DBT_EXECUTABLE = str(Path(sys.executable).parent / "dbt")

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=PROFILES_DIR)
# Under `dagster dev` this re-parses dbt automatically. In production the
# setup script runs `dbt parse` so target/manifest.json exists.
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def tcat_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Every dbt model and test. `dbt build` runs them in dependency order and
    stops downstream models when an error-severity test fails."""
    yield from dbt.cli(["build"], context=context).stream()


@op(retry_policy=RetryPolicy(max_retries=2, delay=60))
def dbt_source_freshness(context: OpExecutionContext, dbt: DbtCliResource):
    """Fails the run if raw data is older than the thresholds in _sources.yml."""
    dbt.cli(["source", "freshness"], manifest=dbt_project.manifest_path).wait()


@job
def freshness_check_job():
    dbt_source_freshness()


# Where failure alerts go: any URL that accepts a POSTed text body, e.g.
# https://ntfy.sh/<a-long-random-topic> (the topic name works like a password).
# Set in /etc/tcat/alerts.env on the server, never in git.
ALERT_URL = os.getenv("TCAT_ALERT_URL")


@run_failure_sensor(default_status=DefaultSensorStatus.RUNNING)
def alert_on_failure(context: RunFailureSensorContext):
    """Push a message when any job fails: a dbt model or error-severity test
    in the daily build, or stale raw data in the hourly freshness check.
    Without this, a failure is only visible in the UI behind an SSH tunnel."""
    text = (f"TCAT pipeline: {context.dagster_run.job_name} failed. "
            f"{context.failure_event.message or ''}")[:1000]
    if not ALERT_URL:
        context.log.warning(f"TCAT_ALERT_URL is not set, so no alert was sent: {text}")
        return
    request = urllib.request.Request(
        ALERT_URL, data=text.encode(), method="POST",
        headers={"Title": "TCAT pipeline failure"})
    # An exception here fails the sensor tick, which Dagster retries next tick.
    with urllib.request.urlopen(request, timeout=10):
        pass


dbt_build_job = define_asset_job(
    name="dbt_build_job",
    selection=AssetSelection.assets(tcat_dbt_assets),
)

defs = Definitions(
    assets=[tcat_dbt_assets],
    jobs=[dbt_build_job, freshness_check_job],
    sensors=[alert_on_failure],
    schedules=[
        ScheduleDefinition(
            job=dbt_build_job,
            cron_schedule="20 6 * * *",
            execution_timezone="America/New_York",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=freshness_check_job,
            cron_schedule="45 * * * *",
            execution_timezone="America/New_York",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ],
    resources={
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=str(PROFILES_DIR),
            dbt_executable=DBT_EXECUTABLE,
        )
    },
)
