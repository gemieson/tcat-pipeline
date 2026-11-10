{{ config(
    partition_by={'field': 'service_date', 'data_type': 'date'},
    cluster_by=['route_id', 'stop_id']
) }}

-- One row per scheduled trip at a watched stop, ready for analysis.
--   * Scope: only the 21 timepoint stops in seeds/watched_stops.csv, not the
--     whole system. Route-level figures mean "at the stops we watch".
--   * Analysis window starts when collection moved to the server.
--   * Only trips whose scheduled arrival has passed: future trips can't
--     have arrived yet, so they'd look like misses.
--   * Delay uses TCAT's recorded arrival (ATA) when present, otherwise the
--     calibrated GPS arrival. arrival_source says which.
-- On time = 1 min early to 5 min late, a common transit definition.

select
    service_date,
    format_date('%A', service_date)                                        as day_of_week,
    extract(dayofweek from service_date) not in (1, 7)                     as is_weekday,
    extract(hour from datetime(scheduled_arrival_at, 'America/New_York'))  as hour_eastern,

    stop_id,
    stop_name,
    route_id,
    direction,
    trip_id,
    gtfs_trip_id,
    headsign,

    scheduled_arrival_at,
    actual_arrival_at,
    best_arrival_at,
    arrival_source,
    last_estimated_arrival_at,
    has_tcat_actual,
    has_gps_arrival,
    arrival_source is not null                                             as is_measured,

    delay_seconds,
    round(delay_seconds / 60, 1)                                           as delay_minutes,
    delay_seconds between -60 and 300                                      as is_on_time,
    tcat_delay_seconds,
    gps_delay_seconds,
    gps_lag_seconds,
    prediction_error_seconds,
    tcat_dev_seconds,
    polls_seen

from {{ ref('int_stop_arrivals_filled') }}
where service_date >= '{{ var("analysis_start_date", "2026-09-21") }}'
  and scheduled_arrival_at < timestamp_sub(current_timestamp(), interval 30 minute)
