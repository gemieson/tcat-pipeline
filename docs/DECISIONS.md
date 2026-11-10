# Engineering decisions

Each entry: the problem, the options considered, the choice and why, what it
costs, and when to revisit it. Finding IDs (REL-01 etc.) refer to the audit of
2026-10-25. Decided 2026-11-03.

---

## D1. Consumer: flush every buffer, then commit once

**Problem.** One buffer per topic, and a commit after each topic's upload. But
`commit()` with no arguments commits every assigned partition in both topics,
so a vehicle upload also marked in-memory departures as done. Reproduced: a
hard kill after a vehicle flush lost departures for good (REL-01). Two
commits in one loop raised `_NO_OFFSET`, crashed the process, and shutdown
re-uploaded a batch (REL-02).

**Options.**
A. Ignore `_NO_OFFSET`: stops the crash, keeps the loss.
B. Upload every non-empty buffer, then commit once.
C. Commit explicit per-partition offsets for the topic just written.
D. One consumer process per topic.
E. An off-the-shelf sink (e.g. Redpanda Connect).

**Decision: B.** One invariant, easy to state and test: at commit time every
buffer is empty, so "committed" means "in GCS". On the crash path the consumer
neither retries nor commits; uncommitted data is re-read on restart. The
revoke callback (partitions moving to another consumer, or shutdown) flushes
and commits first, and is disabled after a crash, since `close()` also calls it.

**Trade-offs.** Some small files: a full vehicle buffer also flushes a few
departures. C is the general design (sink connectors do it) and right when
topics need separate flush timing; here it adds offset bookkeeping for no
gain. D doubles processes on a 1 GB VM. E is what a team would likely use,
but takes away the part of the project that shows the semantics.

**Evidence.** `tests/test_consumer.py`: 9 tests pass; 6 fail against the old
consumer, including 36 records lost when a second instance joins.

---

## D2. A duplicate is an identical envelope

**Problem.** "13% duplicates" mixed two things: transport duplicates (the same
message written twice) and repeated GPS fixes (a bus re-reporting the same
position in the next poll, a real observation).

**Kinds of "twice".** (1) Consumer replay after a crash between upload and
commit. (2) Producer retry after a lost acknowledgement: a new offset, same
content. (3) Repeated GPS fix: a different `ingested_at`. (4) The same trip on
many departure boards: not a duplicate.

**Options.**
A. Kafka coordinates (topic/partition/offset) written into each record.
B. A fingerprint of every envelope field, computed in staging.
C. A producer-assigned `event_id`.
D. The business key alone.

**Decision: B in staging for (1) and (2), plus the existing
`(vehicle_id, vehicle_reported_at)` key in intermediate for (3).** A misses
(2), because a retry gets a new offset: an offset identifies a position in the
log, not an event. C is the default for a new system, but history has no IDs,
so B would be needed anyway. D merges (1)-(3), so the replay rate can't be
measured. The fingerprint covers every envelope column in
`sql/01_external_tables.sql` except `dt`/`hr`, because a replay usually lands
in a later file.

**Trade-offs.** Two different events with identical envelopes would collapse
into one. The producer can't create them: every poll stamps its own
`ingested_at`. If something in transit rewrote messages (e.g. `STRIP_DRIVER`
toggled during a replay), B would miss that replay; C would not.

**Correction on record.** The external tables set `ignore_unknown_values=true`,
so a new field wouldn't break them, as first assumed. That made A and C
cheaper, but not better. It also means a renamed envelope field reads as NULL
silently, so staging has `not_null` tests on envelope fields.

**Revisit.** C can be added on top at any time, for tracing events across stages.

---

## D3. One collection path for vehicles and departures

**Problem.** `scripts/collect.sh` polled the same endpoints as the producer
and kept every response on the VM disk: ~33,000 files a day, never deleted
(OPS-01), on the same disk as Redpanda. It uploaded driver names (SEC-01). Its
route snapshot silently stopped after ~10 days when an `ls` glob outgrew the
argument limit (DATA-01). Nothing downstream read it.

**What a backup should protect against.** This one ran on the same VM, the
same network and the same API, so it failed together with the primary. It had
no restore procedure.

**Options.** Keep it and add retention; trim it; move its unique parts into
Python; retire it.

**Decision: trim.** Keep what only it collects: service alerts (5 min), the
daily stops/routes snapshot, and its per-request log for those calls. Route
IDs come from a live call that is read and discarded, so no driver names are
stored. Writes are atomic (`.part`, then rename). A day is marked done only if
every request succeeded; otherwise it retries after an hour.

**Trade-offs.** No second copy of vehicles and departures, and no request log
for them. Moving everything into Python, with a request log for every
endpoint, is the cleaner end state, and part of the request-log work.

---

## D4. Staging as tables, rebuilt in full

**Problem.** Staging and intermediate were views over external JSON tables, so
every model and test re-read raw files, every hour (COST-01).

**Measured (2026-11-03).** 61 queries per build, 24 builds a day, 85-110
GiB/day billed (about $12/month), flat across 14 days. Raw: 12 GiB (90%
departures), growing ~7.5 GiB/month. The audit had estimated ~10x higher.

**Options.**
A. Views, built less often.
B. Staging as plain tables.
C. Incremental staging (rebuild only recent `dt` partitions).
D. Native load jobs.
E. Parquet from the consumer.

**Decision: B, with D5.** Raw is read once per build instead of by every
downstream model and test. Full rebuilds also make D2 simple: every replay is
compared with its original, whatever day it landed. A daily full rebuild is
at most ~12 GiB/day today, inside the free tier.

**Trade-offs.** Cost still grows with history, roughly linearly. C makes it
flat, at the price of a self-healing window, a replay context day and a
full-refresh procedure; not worth it yet.

**Revisit** when raw passes ~30 GiB (about mid-January 2027 at current
growth), or when bytes per build approach 30 GiB. Then move staging to
incremental (insert-overwrite by `dt`, window from the newest loaded partition,
one extra day read for replay matching).

---

## D5. Build daily, check freshness hourly

**Decision.** `dbt build` at 06:20 Eastern (Dagster's `execution_timezone`
keeps it there across DST). The previous 3am-to-3am service day is complete
by then, and buses restart around 6. The freshness check stays hourly: it
reads a day of raw data and is how an outage is detected. The old systemd
timer is set to match, so nothing re-enables hourly builds.

**Trade-offs.** Marts are up to a day old, which a delay-pattern study
doesn't need. A broken model shows up at the next build, so run `dbt build`
by hand after changes; CI runs `dbt parse` on every push.

**Revisit** for a live view: build a small model over the last few hours of
staging, rather than rebuilding all history more often.

---

## D6. Flag GPS arrivals after a reporting gap

**Problem.** An arrival is the first report at a new `LastStop`; its time is
only as precise as the gap since the bus's previous report. After a 10-minute
gap (collector down, GPS dropout), or on a trip's first report, the arrival
time is late by up to the gap, which biases GPS-filled delays late (DATA-03).

**Options.** Flag; drop; estimate within the gap (midpoint, or the moment the
bus was closest to the stop's coordinates); do nothing.

**Decision: flag.** `is_reliable_arrival` = previous report <= 120s earlier
(`max_report_gap_seconds`). Rows are kept; the GPS fill uses reliable arrivals
only.

**Trade-offs.** Lower fill coverage: trips seen only across a gap become
unmeasured instead of measured wrong. The location-based estimate is the
better long-term method.

---

## D7. On storage failure: crash, and let Kafka hold the data

**Options.** Crash and let systemd restart it; retry inside the process with
backoff; spill to local disk.

**Decision: crash.** The GCS client retries brief errors itself. If it still
fails, the consumer exits without committing, systemd restarts it after 10s,
and Redpanda holds everything uncommitted (default retention ~7 days).
In-process retry would have to keep polling Kafka: if it stops for more than 5
minutes (`max.poll.interval.ms`), Kafka assumes it died and reassigns its
partitions, so buffers must be paused and bounded. That's more code beside
the commit logic. Spilling to disk duplicates what Kafka already does, on the
disk whose filling D3 removed.

**Also decided.**
- **Create-only uploads** (`if_generation_match=0`) with a random suffix, so a
  retry can never overwrite. An "already exists" after a lost response counts
  as written.
- **Quarantine.** A message that isn't a UTF-8 JSON object goes to
  `gs://…/quarantine/` with its topic, partition, offset and bytes, instead of
  crashing the consumer on every restart (REL-05). A dead-letter topic would
  need its own consumer; a prefix you can `ls` is enough here.

**Trade-offs.** A long outage is a noisy restart loop. The freshness check
plus alerts (D8) is how you hear about it.

---

## D8. Failure alerts through a webhook

**Decision.** A Dagster run-failure sensor POSTs to `TCAT_ALERT_URL` (e.g. an
ntfy.sh topic), read from `/etc/tcat/alerts.env`, never from git. With no URL
set, it logs a warning. Stdlib only.

**Trade-offs.** Covers failed builds and stale data (the freshness check), but
not the VM being down entirely, since Dagster runs on it. A free external
uptime check, or Cloud Monitoring, would cover that. Email over SMTP needs
credentials to manage; Cloud Monitoring is more setup.

---

## D9. The watched-stop list lives in the dbt seed only

**Decision.** The producer reads `dbt/tcat/seeds/watched_stops.csv`. One list
means a stop can't be polled but missing from the GPS match, or the reverse.

---

## D10. Redpanda setup as code: pinned, bounded, enforced

**Problem.** The image was `latest`, so any restart that pulled could
silently upgrade the broker. Retention was the default (7 days by time, no
size limit), never chosen. Nothing was compressed. Topic creation hid every
error (`>/dev/null 2>&1 || true`), and re-running setup never updated an
existing topic's config.

**Measured on the VM (2026-11-03).** Redpanda v26.2.3; 30 GB disk, 69% used,
8.7 GB free; inodes 42% used.

**Options.** Leave as is; pin, bound and enforce with `rpk` in a script;
Terraform.

**Decision.** `deploy/topics.sh` creates missing topics and re-applies config
on every run, failing on any real error. The image is pinned to v26.2.3 (VM
and laptop). Retention: 7 days, plus 512 MiB per partition as a backstop. The
producer compresses with zstd. Continuous partition and core balancing,
Enterprise features with nothing to balance on one broker, are switched off.

**Why these numbers.** Redpanda is a buffer in front of GCS, so retention
only has to outlast an outage nobody noticed. Alerts fire within ~6h, so 7
days is generous. The cap is sized for the worst partition: departures hash 21
stops into 3 partitions, so one may carry ~40%. That's ~150 MiB in 7 days at
5x compression, ~385 MiB at 2x, ~770 MiB uncompressed. In the last case the
cap trims retention to ~4.5 days instead of filling the disk. All six
partitions at the cap total 3 GiB.

**Trade-offs.** In a very long backlog, the cap deletes the oldest unconsumed
data rather than stopping the broker. Single broker, no replication: accepted
at this scale. GCS is the durable copy.

**Verify.** A week after deploy, `rpk cluster logdirs describe` shows actual
sizes per partition, and so the real compression ratio.

---

## D11. A request log: one row per API call, written locally

**Problem.** Nothing recorded each API request in the Kafka path. "No buses
were running", "TCAT's API errored" and "the producer was down" all looked
the same in the data: missing rows.

**Options.**
A. Log lines in the journal: not queryable, and rotated.
B. A third Kafka topic.
C. Append to a local file beside the side collector's log, uploaded by the
   existing hourly sync.

**Decision: C.** The producer writes one JSON line per call to
`data/raw/_meta/dt=…/producer.ndjson`: endpoint, stop, HTTP status, latency,
error, records received, produced, delivered, failed, skipped, and the
`ingested_at` its envelopes carry. The collector's `meta.ndjson` sits in the
same folder (now tagged `writer: collector`), so one external table,
`ext_request_log` (`sql/02_request_log.sql`), covers both. B was rejected
because when Redpanda is down, the log of the outage would be lost with it,
and that's exactly when the log is needed.

**Trade-offs.** Up to an hour's delay (the sync). If the VM disk is lost, the
last hour is lost with it. The sync can copy today's file mid-append, so the
external table tolerates up to 10 bad lines (`max_bad_records`); the next sync
re-uploads the file whole. After each successful sync, local log days older
than 14 are deleted. A logging failure (disk full) warns but never stops
collection: the log is the audit trail, Kafka is the data path.

---

## D12. Reconciliation: delivered to Kafka vs landed in BigQuery

**Problem.** Nothing could answer "did we lose data last Tuesday?"

**Options.** A dbt health model with tests; live metrics (Prometheus, Cloud
Monitoring); an ad-hoc query.

**Decision: dbt.** `mart_pipeline_health` has one row per UTC hour per stream,
built on an hour spine, so an hour with no polls at all still appears (with 0).
- `tests/assert_no_loss_kafka_to_bigquery.sql` (error): staged < delivered.
  This should never happen, because the producer is idempotent and staging
  removes replays, so any shortfall is real loss between Kafka and GCS.
- `tests/assert_collection_complete.sql` (warn): successful polls < 90% of
  expected (120/hour for vehicles, 60 × stops for departures).

Both sides count hours by the envelope's `ingested_at`, so a poll straddling
an hour boundary isn't split. Only complete hours are judged: not the first
(the log started mid-hour), not the last 3 (consumer flush, hourly sync, daily
build). Only the last 2 days: a loss pages on the next two builds, then stays
visible in the mart without failing every build forever.

**Trade-offs.** Loss is detected within a day (the daily build), not in
minutes. Live metrics would be faster, but they're another service to run.
Starts at the deploy: older polls have no request log.

**Interview line.** "Every hour, I reconcile what the producer says the broker
confirmed against what landed in BigQuery. Any gap fails the build and pages me."

---

## D13. A trip is TripId + direction

**Problem.** Found in a live API sample (2026-11-03): `TripId` is the trip's
scheduled start time (trip 2145 starts 21:45, 2210 at 22:10), not a unique ID.
On route 30, the inbound and outbound buses that both left at 21:15 shared
TripId 2115. Two places assumed otherwise:
- `int_stop_actuals` grouped by stop + route + TripId, so two opposite-direction
  trips on one board would merge into one row.
- The GPS fill matched on route + TripId + stop name. At a stop name served
  both ways (the two Robert Purcell stops share one), it could take the other
  bus's arrival. Its comment said this couldn't happen.

**Options.** Add direction to the key; key board trips on `GtfsTripId`, which
is unique but missing from the vehicle feed, so the GPS join still needs
direction; leave it.

**Decision.** Staging extracts `Trip.TripDirection`. The trip grain becomes
service day + stop + route + TripId + direction, in `int_stop_actuals`,
`int_stop_arrivals_filled` and `fct_stop_arrivals` (and their uniqueness
tests). The GPS fill also requires the bus's `Direction` to match.

**Evidence.** A unit test reproduces the live case: two buses with TripId 2115,
and the wrong one closer to schedule. Run on sample rows, the old logic picked
the outbound bus (02:29 UTC); the new logic picks the inbound one (02:36).

**Trade-offs.** If a route's direction codes differ between the two feeds (the
sample shows the same style, e.g. "I"/"O"), matches for that route drop out.
That's fail-safe: fewer GPS fills rather than wrong ones. **Verify** after
deploy: GPS fill coverage by route, before vs after.

**Interview line.** "Real data disproved a documented assumption: trip IDs were
start times, shared across directions. I found it in a raw sample, reproduced
it in a unit test, and fixed the grain."

---

## Deferred

- **GTFS schedule as the trip spine (DATA-02).** Trips missed during outages
  should count as unmeasured, not vanish.
- **Per-week GPS lag calibration (DATA-04); prediction error at fixed horizons (DATA-05).**
