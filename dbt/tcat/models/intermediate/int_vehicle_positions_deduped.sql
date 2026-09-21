select
    *,
    timestamp_diff(ingested_at, vehicle_reported_at, second)        as lag_seconds,
    timestamp_diff(ingested_at, vehicle_reported_at, second) > 120  as is_stale
from {{ ref('stg_vehicle_positions') }}
where vehicle_id is not null
  and vehicle_reported_at is not null
qualify row_number() over (
    partition by vehicle_id, vehicle_reported_at
    order by ingested_at
) = 1