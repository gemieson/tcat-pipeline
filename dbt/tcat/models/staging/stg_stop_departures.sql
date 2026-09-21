-- One row per predicted trip per stop per poll. Rename, type, parse --
-- no filtering, no dedup. The same trip appears in many polls; picking the
-- right one happens in the intermediate layer.

with src as (
    select * from {{ source('tcat_raw', 'ext_stop_departures') }}
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
    gtfs_trip_id,
    headsign,
    stop_status,

    -- .NET dates: take the digits (UTC millis), ignore the offset.
    -- Departure fields (SDT/EDT). ADT is never populated by TCAT (0 of 146,839
    -- rows checked), so it is not parsed.
    timestamp_millis(safe_cast(regexp_extract(sdt_raw, r'/Date\((-?\d+)') as int64)) as scheduled_at,
    timestamp_millis(safe_cast(regexp_extract(edt_raw, r'/Date\((-?\d+)') as int64)) as estimated_at,

    -- Arrival fields (STA/ETA/ATA). ATA IS populated: it appears once the bus
    -- arrives, stays ~4 polls, and never changes. This is the actual time.
    timestamp_millis(safe_cast(regexp_extract(sta_raw, r'/Date\((-?\d+)') as int64)) as scheduled_arrival_at,
    timestamp_millis(safe_cast(regexp_extract(eta_raw, r'/Date\((-?\d+)') as int64)) as estimated_arrival_at,
    timestamp_millis(safe_cast(regexp_extract(ata_raw, r'/Date\((-?\d+)') as int64)) as actual_arrival_at,

    -- "00:02:48" -> 168. Handles a leading minus if TCAT ever sends one.
    -- Unparseable values become null rather than failing the whole model.
    dev_raw,
    case
        when dev_raw is null then null
        else (case when starts_with(dev_raw, '-') then -1 else 1 end) * (
              safe_cast(split(ltrim(dev_raw, '-'), ':')[safe_offset(0)] as int64) * 3600
            + safe_cast(split(ltrim(dev_raw, '-'), ':')[safe_offset(1)] as int64) * 60
            + safe_cast(split(ltrim(dev_raw, '-'), ':')[safe_offset(2)] as int64)
        )
    end                                                             as dev_seconds

from extracted
