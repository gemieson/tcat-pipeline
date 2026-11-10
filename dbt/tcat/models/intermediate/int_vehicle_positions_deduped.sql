-- One row per real bus report. A bus that hasn't sent a new GPS fix appears
-- unchanged in consecutive polls; keep its first sighting. route_id breaks the
-- tie when one report is listed under two routes in the same poll, so every
-- run keeps the same row.
select
    *,
    timestamp_diff(ingested_at, vehicle_reported_at, second)        as lag_seconds,
    timestamp_diff(ingested_at, vehicle_reported_at, second) > 120  as is_stale
from {{ ref('stg_vehicle_positions') }}
where vehicle_id is not null
  and vehicle_reported_at is not null
qualify row_number() over (
    partition by vehicle_id, vehicle_reported_at
    order by ingested_at, route_id
) = 1
