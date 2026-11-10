-- Request log: one line per API call, from the producer (producer.ndjson:
-- vehicles and departures, with delivery counts) and the side collector
-- (meta.ndjson: alerts and daily dims). Written locally, synced to GCS hourly
-- by tcat-sync (docs/DECISIONS.md, D11). Run this once, before deploying the
-- dbt models that read it; it is metadata only, so nothing is scanned.
--
-- max_bad_records: the hourly sync can copy today's file mid-append, leaving
-- one cut-off last line. The next sync replaces the file whole, so up to a
-- few bad lines per query are tolerated rather than failing the query.

CREATE OR REPLACE EXTERNAL TABLE `tcat-pipeline.tcat_raw.ext_request_log`
(
  ingest_ts STRING,
  writer STRING,
  producer_version STRING,
  source STRING,
  url STRING,
  stop_id INT64,
  route_id INT64,
  http_code INT64,
  duration_s FLOAT64,
  bytes INT64,
  stored BOOL,
  error STRING,
  records INT64,
  produced INT64,
  delivered INT64,
  failed INT64,
  skipped INT64,
  envelope_ingested_at STRING
)
WITH PARTITION COLUMNS (dt DATE)   -- from the dt=YYYY-MM-DD folder names
OPTIONS(
  ignore_unknown_values=true,
  max_bad_records=10,
  format="NEWLINE_DELIMITED_JSON",
  hive_partition_uri_prefix="gs://tcat-pipeline-tcat-raw/bash/raw/_meta",
  uris=["gs://tcat-pipeline-tcat-raw/bash/raw/_meta/*"]
);
