-- One row per API call (docs/DECISIONS.md, D11). Two writers share the log:
--   producer:  vehicles and departures, with records received/produced/
--              delivered/failed/skipped per call
--   collector: service alerts and the daily stops/routes snapshot
-- Lines are appended locally and the whole file is re-uploaded each hour, so
-- there are no replays to remove.

select
    dt,
    timestamp(ingest_ts)                    as requested_at,
    -- collector lines from before Nov 2026 have no writer field
    coalesce(writer, 'collector')           as writer,
    producer_version,
    source,
    url,
    stop_id,
    route_id,
    http_code,
    duration_s,
    bytes,
    stored,
    error,
    records,
    produced,
    delivered,
    failed,
    skipped,
    timestamp(envelope_ingested_at)         as envelope_ingested_at
from {{ source('tcat_raw', 'ext_request_log') }}
