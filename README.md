# TCAT bus delay pipeline

Measures how late Ithaca's TCAT buses run, by route and hour of day, using TCAT's
public real-time feed (Avail InfoPoint API).

## How it works

```
TCAT API ──> producer (every 30s / 60s) ──> Redpanda (Kafka) ──> consumer ──> GCS (NDJSON, dt=/hr= partitions)
                                                                                 │
                                                       BigQuery external tables (tcat_raw)
                                                                                 │
                                         dbt: staging ─> intermediate ─> marts (tcat_marts)
                                                                                 │
                                   Dagster: daily `dbt build`, hourly freshness check, failure alerts
```

Everything runs on one small Compute Engine VM (`tcat-pipeline`, us-east1-b) under systemd.

| Piece | File | What it does |
|---|---|---|
| Producer | `producer/produce.py` | Polls vehicle positions (30s) and departure boards for 21 timepoint stops (60s). Publishes raw payloads to `tcat.vehicle_positions.v1` and `tcat.stop_departures.v1`. |
| Consumer | `consumer/consume.py` | Buffers each topic, writes NDJSON to GCS, and flushes both topics together before committing offsets once, so a crash can cause duplicates (removed in staging) but never gaps. Create-only writes; messages that aren't JSON objects go to `quarantine/`. `STRIP_DRIVER=1` removes driver names. |
| Side collector | `scripts/collect.sh` | Bash poller for what Kafka doesn't carry: service alerts every 5 min and a daily snapshot of stops and route details. Synced to `gs://…/bash/` hourly. |
| dbt | `dbt/tcat/` | Turns raw JSON into analysis tables (below). |
| Orchestration | `orchestration/definitions.py` | Dagster: `dbt build` daily at 06:20 Eastern, `dbt source freshness` hourly at :45, and a push alert when any run fails (`TCAT_ALERT_URL`). |
| Tests | `tests/`, `dbt/tcat/tests/` | pytest for producer, consumer and collector (no Docker or cloud needed); dbt unit and data tests. See [Tests](#tests). |
| Deploy | `deploy/` | VM setup scripts, systemd units, Redpanda compose file (pinned v26.2.3). `topics.sh` holds topic config as code: 3 partitions, 7-day retention, 512 MiB per-partition cap. |
| BigQuery DDL | `sql/` | External tables: raw streams (`01`) and the request log (`02`). |
| Docs | `docs/` | `DECISIONS.md` (design decisions and trade-offs), `RUNBOOK.md` (replay, recovery, incidents). |
| Local dev | `docker-compose.yml` | Redpanda + Console on your laptop. |
| API explorer | `scripts/probe_all.py` | One-off: hits each InfoPoint endpoint and prints its shape. |

### dbt models

- **staging** – `stg_vehicle_positions`, `stg_stop_departures`: unpack JSON, parse .NET `/Date(ms)/` timestamps and `Dev` ("00:02:48") into seconds, and drop exact replays (identical envelope = the same message written twice). Tables, rebuilt in full each build, so raw JSON is read once per build.
- **intermediate**
  - `int_vehicle_positions_deduped` – drop repeat GPS reports, flag stale ones
  - `int_vehicle_stop_arrivals` – one row each time a bus reaches a new stop (GPS); `is_reliable_arrival` is false after a reporting gap over 120s and on a trip's first report
  - `int_stop_actuals` – one row per trip (TripId + direction: TripId is the start time, shared across directions) per stop per day from the departure boards, with TCAT's recorded arrival (ATA)
  - `int_stop_arrivals_filled` – the above with missing arrivals filled from reliable GPS arrivals, calibrated for GPS running late
- **health** – `stg_request_log` (one row per API call, from the producer and side collector) and `mart_pipeline_health` (per hour: polls made vs expected, messages delivered to Kafka vs rows in BigQuery; a shortfall fails the build).
- **marts** – `fct_stop_arrivals` (past trips at watched stops from 2026-09-21, on time = 1 min early to 5 min late) and `mart_route_hourly_delay` (avg/median delay, % on time, prediction error by route × hour, plus the TCAT-only on-time rate for comparison).
- **seeds** – `watched_stops.csv`, the 21 stops in scope, with the names the GPS feed uses.
- **analyses** – `ata_missingness_check.sql`, compares trips TCAT recorded vs missed.

All service dates use the `service_date()` macro: the service day starts at 3am Eastern, so late-night trips belong to the day they started.

### Scope and data quality

- **Scope is 21 timepoint stops, not the whole system.** "Route 11 is 4 minutes late at 5pm" means at the stops in `seeds/watched_stops.csv`.
- **TCAT never fills ADT** (actual departure). It fills **ATA** (actual arrival) for only part of the trips: about 54% in the Sep 18–21 laptop data, which had collection gaps.
- **GPS fill.** Missing arrivals are filled from the GPS feed by matching service day + route + trip + stop name. In the Sep 18–21 data GPS matched 98.6% of trips that had an ATA and filled 38% of those that didn't, raising coverage from 54% to 72%.
- **GPS calibration.** A bus's `LastStop` only changes after it leaves the stop, so GPS times trail ATA (median +54s, 90th percentile +161s). The median lag is re-measured every run from trips that have both, and subtracted.
- **Is ATA missing at random?** In Sep 18–21, trips without ATA were not later than trips with it (median 3.6 vs 3.4 min late, by GPS). Re-check on the full window with `analyses/ata_missingness_check.sql`.

## Delivery guarantees

Stated so they can be checked, not assumed:

| Hop | Guarantee | How |
|---|---|---|
| TCAT API → producer | Best effort | A missed poll can't be re-polled. Each poll logs `produced / delivered / failed / skipped / api_errors`. |
| Producer → Redpanda | At-least-once per poll, failures counted | Delivery reports, idempotent producer, 20s delivery timeout. |
| Redpanda → GCS | At-least-once | All buffers uploaded, then one commit. A crash between upload and commit re-delivers that batch. |
| GCS → staging | Exact replays removed | A replay has a byte-identical envelope (`macros/replay_dedup.sql`). |
| Staging → intermediate | Repeated GPS fixes collapsed | Same vehicle + same report time = one observation (`int_vehicle_positions_deduped`). |

Checked, not assumed: `mart_pipeline_health` reconciles, per hour, what the broker confirmed against what reached staging, and a shortfall fails the build.

Not exactly-once: a Kafka offset commit and a GCS write can't be one transaction. The results are effectively-once because every duplicate path ends in a deterministic dedup.

## Tests

```bash
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest -v          # ~2 min; starts a mock Kafka broker in-process
```

dbt unit and data tests run in BigQuery as part of `dbt build`, or alone with `dbt test`.
CI (`.github/workflows/tests.yml`) runs pytest and `dbt parse` on every push.

## Deploying on the VM

```bash
bash deploy/setup.sh          # swap, docker, python venv, Redpanda + topics, producer/consumer/collector/sync
bash deploy/setup_dbt.sh      # dbt venv + BigQuery profile (uses the VM's service account, no key file)
bash deploy/setup_dagster.sh  # Dagster takes over hourly dbt (disables the old tcat-dbt timer)
bash deploy/status.sh         # health check
```

`tcat-dbt.service/.timer` are the pre-Dagster scheduler; `setup_dbt.sh` installs them and `setup_dagster.sh` turns them off.

Failure alerts: put `TCAT_ALERT_URL=https://ntfy.sh/<long-random-topic>` in `/etc/tcat/alerts.env` (`sudo chmod 600`), then `sudo systemctl restart tcat-dagster-daemon`. Subscribe to the same topic in the ntfy app.

Dagster UI: `gcloud compute ssh tcat-pipeline --zone=us-east1-b -- -N -L 3000:localhost:3000`, then http://localhost:3000.

## Known gaps

- **External table DDL is missing.** `_sources.yml` points to `sql/01_external_tables.sql`, which isn't in the project. The `tcat_raw.ext_vehicle_positions` / `ext_stop_departures` tables exist in BigQuery, but the SQL that created them should be saved here.
- **Trips never seen on a board are missing, not unmeasured.** During a collection outage, `scheduled_trips` and `pct_measured` only count trips that appeared on some poll. Fixing this needs TCAT's GTFS schedule as the list of trips that should have run.
- **The dbt changes of 2026-11-03 were checked offline, not yet run in BigQuery** (parsed by dbt; SQL checked for BigQuery syntax; logic run on sample rows in DuckDB). Their unit tests run with the first `dbt build`.
- **Staging is rebuilt in full each day.** About 12 GiB of raw data and ~7.5 GiB/month of growth as of Nov 2026; revisit incremental staging when raw passes ~30 GiB (docs/DECISIONS.md, D4).
- The stop list lives in `seeds/watched_stops.csv` only; the producer reads it.
- GPS lag calibration is re-estimated over all history each run, so past delay figures can shift by seconds.
