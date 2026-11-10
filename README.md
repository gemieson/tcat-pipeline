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
                                                    Dagster: hourly `dbt build` + freshness check
```

Everything runs on one small Compute Engine VM (`tcat-pipeline`, us-east1-b) under systemd.

| Piece | File | What it does |
|---|---|---|
| Producer | `producer/produce.py` | Polls vehicle positions (30s) and departure boards for 21 timepoint stops (60s). Publishes raw payloads to `tcat.vehicle_positions.v1` and `tcat.stop_departures.v1`. |
| Consumer | `consumer/consume.py` | Buffers each topic and writes NDJSON to GCS. Commits offsets only after a successful upload, so a crash causes duplicates (removed in dbt) instead of gaps. `STRIP_DRIVER=1` removes driver names. |
| Backup collector | `scripts/collect.sh` | Independent bash poller that saves raw gzipped responses to disk, plus service alerts and a daily snapshot of stops/routes. Synced to `gs://…/bash/` hourly. |
| dbt | `dbt/tcat/` | Turns raw JSON into analysis tables (below). |
| Orchestration | `orchestration/definitions.py` | Dagster: `dbt build` at :15 and `dbt source freshness` at :45 every hour. |
| Deploy | `deploy/` | VM setup scripts, systemd units, Redpanda compose file. |
| Local dev | `docker-compose.yml` | Redpanda + Console on your laptop. |
| API explorer | `scripts/probe_all.py` | One-off: hits each InfoPoint endpoint and prints its shape. |

### dbt models

- **staging** – `stg_vehicle_positions`, `stg_stop_departures`: unpack JSON, parse .NET `/Date(ms)/` timestamps and `Dev` ("00:02:48") into seconds.
- **intermediate**
  - `int_vehicle_positions_deduped` – drop repeat GPS reports, flag stale ones
  - `int_vehicle_stop_arrivals` – one row each time a bus reaches a new stop (GPS)
  - `int_stop_actuals` – one row per trip per stop per day from the departure boards, with TCAT's recorded arrival (ATA)
  - `int_stop_arrivals_filled` – the above with missing arrivals filled from GPS, calibrated for GPS running late
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

## Deploying on the VM

```bash
bash deploy/setup.sh          # swap, docker, python venv, Redpanda + topics, producer/consumer/collector/sync
bash deploy/setup_dbt.sh      # dbt venv + BigQuery profile (uses the VM's service account, no key file)
bash deploy/setup_dagster.sh  # Dagster takes over hourly dbt (disables the old tcat-dbt timer)
bash deploy/status.sh         # health check
```

`tcat-dbt.service/.timer` are the pre-Dagster scheduler; `setup_dbt.sh` installs them and `setup_dagster.sh` turns them off.

Dagster UI: `gcloud compute ssh tcat-pipeline --zone=us-east1-b -- -N -L 3000:localhost:3000`, then http://localhost:3000.

## Known gaps

- **External table DDL is missing.** `_sources.yml` points to `sql/01_external_tables.sql`, which isn't in the project. The `tcat_raw.ext_vehicle_positions` / `ext_stop_departures` tables exist in BigQuery, but the SQL that created them should be saved here.
- **Consumer can crash on commit.** When both topics flush in the same loop, the second `consumer.commit()` can raise `_NO_OFFSET` ("No offset stored"). systemd restarts it and nothing is lost, but wrapping that commit to ignore `_NO_OFFSET` would stop the restarts.
- The stop list lives in three places: `producer/produce.py`, `scripts/collect.sh` and `seeds/watched_stops.csv`. Keep them in sync.
- The GPS-fill models have been syntax-checked but not yet run against BigQuery. After deploying, run `bash deploy/setup_dagster.sh` (re-parses dbt) and check the first `dbt build`.
- Uncommitted work (Dagster, renamed intermediate models, tests) – run `git status` and commit.
