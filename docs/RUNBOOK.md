# Runbook

What to do when something breaks, and how to re-land data. Run on the VM from
the project folder unless noted. `RPK` below is shorthand for:

```bash
RPK="sudo docker compose -f deploy/docker-compose.vm.yml exec -T redpanda rpk"
```

## Is it healthy?

```bash
bash deploy/status.sh        # services, memory, disk + inodes, lag, quarantine, newest GCS files
```

Healthy looks like:
- all services `active`
- producer lines with `failed=0`
- consumer lag of a few hundred messages at most
- `quarantine` empty
- newest GCS file under 5 minutes old

## Did we lose anything? (request log and reconciliation)

```sql
-- hours with a problem in the last week (tcat_marts.mart_pipeline_health)
select hour, stream, expected_requests, requests, requests_ok, delivered, staged, missing_in_bigquery
from `tcat-pipeline.tcat_marts.mart_pipeline_health`
where is_checkable and hour >= timestamp_sub(current_timestamp(), interval 7 day)
  and (missing_in_bigquery > 0 or requests_ok < 0.9 * expected_requests)
order by hour desc;

-- what the API returned during one of those hours
select requested_at, source, stop_id, http_code, error, records, delivered, failed
from `tcat-pipeline.tcat_staging.stg_request_log`
where writer = 'producer' and requested_at between '2026-11-10 14:00' and '2026-11-10 15:00'
order by requested_at;
```

- **`missing_in_bigquery > 0`**: the broker confirmed messages that never
  reached staging. Look at consumer logs and `quarantine/` for that window. If
  it's within Redpanda retention, re-land it (below).
- **`requests_ok` low**: TCAT errors (`http_code`, `error`) or the producer
  down (no rows). Missed polls can't be recovered; the mart is the record.

## Alerts and what they mean

| Alert | Likely cause | First step |
|---|---|---|
| `freshness_check_job failed` | No new raw data for 6h: producer, consumer, Redpanda or VM down; or TCAT API down | `bash deploy/status.sh` |
| `dbt_build_job failed` | A model or error-severity test failed, including data lost between Kafka and BigQuery (`assert_no_loss_kafka_to_bigquery`) | Dagster UI (tunnel in README) → failed step's log; for a loss, see the section above |
| No alerts at all, but data stale | The VM itself is down (alerts run on it) | `gcloud compute instances describe tcat-pipeline --zone us-east1-b` |

## Producer: `failed=N` in its log

Messages didn't reach Redpanda within 20s; those polls are lost, and the count
says how much.

```bash
$RPK cluster health
sudo docker compose -f deploy/docker-compose.vm.yml up -d
journalctl -u tcat-producer -n 20 --no-pager
```

If every send fails right after a deploy, check that Redpanda allows
idempotent producers (on by default): `$RPK cluster config get enable_idempotence`.

## Consumer: restart loop

```bash
journalctl -u tcat-consumer -n 50 --no-pager
```

- **GCS errors:** expected while GCS or credentials are failing. Nothing is
  committed, so nothing is lost. Records wait in Redpanda until it recovers.
  Check credentials with `gcloud storage ls gs://tcat-pipeline-tcat-raw/`.
- **Lag:** `$RPK group describe tcat-gcs-sink` shows lag per partition. It
  should fall quickly once the consumer is healthy.
- **How long can it be down?** Redpanda's retention, about 7 days by default
  (check with `$RPK topic describe tcat.vehicle_positions.v1 -c`). Past that,
  unconsumed data is gone.

## Topic config and disk use

Topic settings live in `deploy/topics.sh` (retention 7 days, 512 MiB per
partition). To change one, edit the script and re-run it: it re-applies config
to existing topics.

```bash
bash deploy/topics.sh
$RPK topic describe tcat.stop_departures.v1 -c | grep retention
$RPK cluster logdirs describe          # bytes on disk per partition
```

## Quarantined messages

```bash
gcloud storage ls "gs://tcat-pipeline-tcat-raw/quarantine/**"
gcloud storage cat "gs://tcat-pipeline-tcat-raw/quarantine/dt=…/hr=…/….jsonl" | head
```

Each line has the topic, partition, offset, error and the message bytes
(`value_b64`). Nothing reads `quarantine/` automatically. Fix the producer if
it's sending bad data; decide case by case whether to re-land anything.

## Re-land a time window from Redpanda (within retention)

For example, files were deleted from GCS, or a consumer bug wrote bad output.
Safe because exact replays are removed in staging (DECISIONS.md, D2).

```bash
sudo systemctl stop tcat-consumer                 # the group must have no active members
$RPK group seek tcat-gcs-sink --to 1793714400     # unix seconds of the window start; see `$RPK group seek --help`
sudo systemctl start tcat-consumer                # re-reads from there and writes new files
```

Then rebuild: `cd dbt/tcat && ../../.venv-dbt/bin/dbt build`.

## Rebuild BigQuery from raw files

Staging is rebuilt in full every build, so after any model fix:

```bash
cd dbt/tcat && ../../.venv-dbt/bin/dbt build
```

If an external table is missing or wrong, recreate it from
`sql/01_external_tables.sql`. It's metadata only, so no data is scanned or lost.

## Disk full on the VM

```bash
df -h / && df -i /
sudo du -xh --max-depth=2 / 2>/dev/null | sort -h | tail -15
```

The collector now writes ~300 files a day. Old `data/raw/vehicles` and
`data/raw/departures` from before Nov 2026 can be removed: they're also in
`gs://…/bash/raw`, and the Kafka path holds the same data.

## Unverified until first run

These commands follow `rpk` and `gcloud` documentation but have not been run
against this VM: `group seek`, `cluster config get` and `set`, `cluster logdirs describe`, and `deploy/topics.sh` as a whole (tested only against a fake rpk). Run them once, and fix
this file if your versions differ.
