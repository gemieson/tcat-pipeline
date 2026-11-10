{{ config(severity='warn') }}
-- Warn: fewer than 90% of the expected polls in a complete hour, i.e. the
-- producer was down, slow or erroring. Missed polls can't be recovered, so
-- this measures the gap rather than fixing it.
select hour, stream, requests, requests_ok, expected_requests
from {{ ref('mart_pipeline_health') }}
where is_checkable
  and hour >= timestamp_sub(current_timestamp(), interval 2 day)
  and requests_ok < 0.9 * expected_requests
