-- One row per bus per poll. Rename, type, parse -- no filtering, no dedup.

with src as (
    select * from {{ source('tcat_raw', 'ext_vehicle_positions') }}
)

select
    dt,
    timestamp(ingested_at)                                          as ingested_at,
    producer_version,
    route_id,

    safe_cast(json_value(payload, '$.VehicleId')     as int64)      as vehicle_id,
    safe_cast(json_value(payload, '$.TripId')        as int64)      as trip_id,
    json_value(payload, '$.Direction')                              as direction,
    json_value(payload, '$.LastStop')                               as last_stop_name,

    safe_cast(json_value(payload, '$.Deviation')     as int64)      as deviation,
    json_value(payload, '$.DisplayStatus')                          as display_status,

    safe_cast(json_value(payload, '$.OnBoard')       as int64)      as on_board,
    safe_cast(json_value(payload, '$.TotalCapacity') as int64)      as total_capacity,

    safe_cast(json_value(payload, '$.Latitude')      as float64)    as latitude,
    safe_cast(json_value(payload, '$.Longitude')     as float64)    as longitude,

    -- /Date(1789944938000-0400)/ : the digits are already UTC milliseconds.
    -- The offset is display metadata. Adding it is a silent 4-hour error.
    timestamp_millis(safe_cast(regexp_extract(
        json_value(payload, '$.LastUpdated'), r'/Date\((-?\d+)') as int64
    ))                                                              as vehicle_reported_at

from src
