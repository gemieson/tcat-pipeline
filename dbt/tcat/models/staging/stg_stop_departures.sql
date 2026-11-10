{{ config(partition_by={'field': 'dt', 'data_type': 'date'}) }}

-- One row per predicted trip per stop per poll. Rename, type, parse, and drop
-- exact replays (macros/replay_dedup.sql). The same trip appears in many
-- polls; picking the right one happens in the intermediate layer.

with src as (
    {{ without_replays(source('tcat_raw', 'ext_stop_departures'),
        ['ingested_at', 'producer_version', 'stop_id', 'stop_record_id',
         'stop_last_updated', 'route_id', 'route_record_id',
         'direction', 'direction_code', 'is_done', 'payload']) }}
),

extracted as (
    select
        dt,
        timestamp(ingested_at)                                      as ingested_at,
        producer_version,
        stop_id,
        route_id,
        direction,
        is_done,

        safe_cast(json_value(payload, '$.Trip.TripId') as int64)    as trip_id,
        json_value(payload, '$.Trip.GtfsTripId')                    as gtfs_trip_id,
        -- TripId is the trip's start time (2145 = 21:45), shared by trips in
        -- opposite directions, so a trip is TripId + direction (D13).
        json_value(payload, '$.Trip.TripDirection')                 as trip_direction,
        json_value(payload, '$.Trip.InternalSignDesc')              as headsign,
        json_value(payload, '$.StopStatusReportLabel')              as stop_status,

        json_value(payload, '$.SDT')                                as sdt_raw,
        json_value(payload, '$.EDT')                                as edt_raw,
        json_value(payload, '$.STA')                                as sta_raw,
        json_value(payload, '$.ETA')                                as eta_raw,
        json_value(payload, '$.ATA')                                as ata_raw,
        json_value(payload, '$.Dev')                                as dev_raw
    from src
    )

select
    dt,
    ingested_at,
    producer_version,
    stop_id,
    route_id,
    direction,
    is_done,
    trip_id,
    trip_direction,
    gtfs_trip_id,
    headsign,
    stop_status,

    -- Departure fields (SDT/EDT). ADT is never populated by TCAT (0 of 146,839
    -- rows checked), so it is not parsed.
    {{ dotnet_timestamp('sdt_raw') }}                               as scheduled_at,
    {{ dotnet_timestamp('edt_raw') }}                               as estimated_at,

    -- Arrival fields (STA/ETA/ATA). ATA IS populated: it appears once the bus
    -- arrives, stays ~4 polls, and never changes. This is the actual time.
    {{ dotnet_timestamp('sta_raw') }}                               as scheduled_arrival_at,
    {{ dotnet_timestamp('eta_raw') }}                               as estimated_arrival_at,
    {{ dotnet_timestamp('ata_raw') }}                               as actual_arrival_at,

    dev_raw,
    {{ dev_seconds('dev_raw') }}                                    as dev_seconds

from extracted
