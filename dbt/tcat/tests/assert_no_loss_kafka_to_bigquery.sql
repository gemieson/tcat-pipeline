-- Error: every message the broker confirmed should be in staging. A shortfall
-- in a complete hour means data was lost between Kafka and GCS. Fails the
-- build, which triggers the failure alert.
-- Only the last 2 days: a loss alerts on the next two daily builds, then
-- stays visible in mart_pipeline_health without failing every build forever.
select hour, stream, delivered, staged, missing_in_bigquery
from {{ ref('mart_pipeline_health') }}
where is_checkable
  and hour >= timestamp_sub(current_timestamp(), interval 2 day)
  and missing_in_bigquery > 0
