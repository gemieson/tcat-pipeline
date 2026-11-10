-- External tables over the consumer's raw NDJSON in GCS. Exported from
-- tcat-pipeline.tcat_raw.INFORMATION_SCHEMA.TABLES on 2026-11-03; this file
-- is the record of what exists in BigQuery.
--
-- This is BigQuery's own export, which lists dt and hr in the column list too.
-- To re-create a table, list them only in the partition clause:
--   WITH PARTITION COLUMNS (dt DATE, hr INT64)
--
-- dt and hr come from the Hive-style paths (raw/<stream>/dt=YYYY-MM-DD/hr=HH/).
-- ignore_unknown_values=true: an envelope field the table doesn't define is
-- dropped silently, so a renamed field reads as NULL. Staging has not_null
-- tests on the envelope fields for exactly that reason.

CREATE EXTERNAL TABLE `tcat-pipeline.tcat_raw.ext_vehicle_positions`
(
  ingested_at STRING,
  producer_version STRING,
  route_id INT64,
  route_record_id INT64,
  payload JSON,
  dt DATE,
  hr INT64
)
WITH PARTITION COLUMNS
OPTIONS(
  ignore_unknown_values=true,
  format="NEWLINE_DELIMITED_JSON",
  hive_partition_uri_prefix="gs://tcat-pipeline-tcat-raw/raw/vehicles",
  uris=["gs://tcat-pipeline-tcat-raw/raw/vehicles/*"]
);

CREATE EXTERNAL TABLE `tcat-pipeline.tcat_raw.ext_stop_departures`
(
  ingested_at STRING,
  producer_version STRING,
  stop_id INT64,
  stop_record_id INT64,
  stop_last_updated STRING,
  route_id INT64,
  route_record_id INT64,
  direction STRING,
  direction_code STRING,
  is_done BOOL,
  payload JSON,
  dt DATE,
  hr INT64
)
WITH PARTITION COLUMNS
OPTIONS(
  ignore_unknown_values=true,
  format="NEWLINE_DELIMITED_JSON",
  hive_partition_uri_prefix="gs://tcat-pipeline-tcat-raw/raw/departures",
  uris=["gs://tcat-pipeline-tcat-raw/raw/departures/*"]
);
