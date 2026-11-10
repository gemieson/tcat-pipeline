{{ config(partition_by={'field': 'dt', 'data_type': 'date'}) }}

-- One row per bus per poll. Rename, type, parse, and drop exact replays
-- (macros/replay_dedup.sql). No other filtering: repeated GPS fixes across
-- polls are real observations, collapsed in int_vehicle_positions_deduped.
--
-- Stored as a table, rebuilt in full each build, so the raw JSON in GCS is
-- read once per build instead of by every downstream model and test.

with src as (
    {{ without_replays(source('tcat_raw', 'ext_vehicle_positions'),
        ['ingested_at', 'producer_version', 'route_id', 'route_record_id', 'payload']) }}
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

    {{ dotnet_timestamp("json_value(payload, '$.LastUpdated')") }}  as vehicle_reported_at

from src
