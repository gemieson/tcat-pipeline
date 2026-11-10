-- One row per trip per stop per service day: every scheduled stop visit seen
-- on a departure board, whether or not TCAT recorded the arrival.
--
-- Findings behind this design (checked against raw data, Sep 21):
--   * ADT (actual departure) is never populated. ATA (actual arrival) is.
--   * ATA is recorded for ~65-70% of trips while collection is running.
--   * Once ATA appears it never changes and stays on the board ~4 polls.
-- Trips without ATA are kept (has_tcat_actual = false) so coverage can be
-- measured and filled from the vehicles feed later.

with polls as (
    select
        *,
        -- same 3am cutoff as the GPS side, so the two can be joined
        {{ service_date('scheduled_arrival_at') }} as service_date
    from {{ ref('stg_stop_departures') }}
    where trip_id is not null
      and scheduled_arrival_at is not null
)

, per_visit as (
    select
        service_date,
        stop_id,
        route_id,
        trip_id,
        any_value(direction)            as direction,
        any_value(gtfs_trip_id)         as gtfs_trip_id,
        any_value(headsign)             as headsign,

        min(scheduled_arrival_at)       as scheduled_arrival_at,   -- schedule is fixed
        min(actual_arrival_at)          as actual_arrival_at,      -- ATA never changes once set

        -- TCAT's last prediction before the bus arrived
        array_agg(if(actual_arrival_at is null, estimated_arrival_at, null)
                  ignore nulls order by ingested_at desc limit 1)[safe_offset(0)]
                                        as last_estimated_arrival_at,

        -- TCAT's own lateness figure at the moment of arrival
        array_agg(if(actual_arrival_at is not null, dev_seconds, null)
                  ignore nulls order by ingested_at limit 1)[safe_offset(0)]
                                        as tcat_dev_seconds,

        count(*)                        as polls_seen,
        min(ingested_at)                as first_seen_at,
        max(ingested_at)                as last_seen_at
    from polls
    group by service_date, stop_id, route_id, trip_id
)

select
    *,
    actual_arrival_at is not null                                          as has_tcat_actual,
    -- positive = late, negative = early
    timestamp_diff(actual_arrival_at, scheduled_arrival_at, second)        as delay_seconds,
    timestamp_diff(actual_arrival_at, last_estimated_arrival_at, second)   as prediction_error_seconds
from per_visit
