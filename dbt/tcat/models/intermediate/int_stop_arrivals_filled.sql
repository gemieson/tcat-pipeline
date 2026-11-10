-- int_stop_actuals with missing arrivals filled from the GPS feed.
-- One row per trip per stop per service day (same grain as int_stop_actuals).
--
-- Why: TCAT records an actual arrival (ATA) for only part of the trips. If the
-- trips it misses are systematically later, delay averages built on ATA alone
-- are biased. The GPS feed sees most of those trips too.
--
-- How the GPS side is matched:
--   * watched_stops (seed) maps stop_id -> stop_name, the name the GPS feed
--     reports as LastStop.
--   * Join on service_date + route + trip + stop name. A trip runs one
--     direction, so the two Robert Purcell stops that share a name can't be
--     confused.
--   * A loop route can pass the same stop twice in one trip: take the GPS
--     arrival closest to the scheduled time, within -30..+90 min.
--
-- Calibration: LastStop only changes once the bus has LEFT the stop, so GPS
-- arrival times run late. Checked on Sep 18-21 data: GPS - ATA median +54s,
-- p90 +161s, across 2,316 trips that had both. The offset is re-measured
-- on every run from trips that have both and subtracted from GPS-only trips.

with board as (
    select a.*, s.stop_name
    from {{ ref('int_stop_actuals') }} a
    left join {{ ref('watched_stops') }} s using (stop_id)
)

, gps_candidates as (
    select
        b.service_date,
        b.stop_id,
        b.route_id,
        b.trip_id,
        g.arrived_at as gps_arrived_at
    from board b
    join {{ ref('int_vehicle_stop_arrivals') }} g
      on  g.service_date = b.service_date
      and g.route_id     = b.route_id
      and g.trip_id      = b.trip_id
      and g.stop_name    = b.stop_name
    where timestamp_diff(g.arrived_at, b.scheduled_arrival_at, minute) between -30 and 90
    qualify row_number() over (
        partition by b.service_date, b.stop_id, b.route_id, b.trip_id
        order by abs(timestamp_diff(g.arrived_at, b.scheduled_arrival_at, second))
    ) = 1
)

, joined as (
    select b.*, g.gps_arrived_at
    from board b
    left join gps_candidates g
      using (service_date, stop_id, route_id, trip_id)
)

, calibration as (
    -- median lag of GPS behind TCAT's recorded arrival, in seconds
    select
        coalesce(
            approx_quantiles(timestamp_diff(gps_arrived_at, actual_arrival_at, second), 2)[safe_offset(1)],
            0
        ) as gps_lag_seconds
    from joined
    where actual_arrival_at is not null
      and gps_arrived_at is not null
)

select
    j.* except (delay_seconds),

    j.gps_arrived_at is not null                                   as has_gps_arrival,
    c.gps_lag_seconds,

    -- best available arrival: TCAT's own record first, calibrated GPS second
    coalesce(
        j.actual_arrival_at,
        timestamp_sub(j.gps_arrived_at, interval c.gps_lag_seconds second)
    )                                                              as best_arrival_at,

    case
        when j.actual_arrival_at is not null then 'tcat'
        when j.gps_arrived_at    is not null then 'gps'
    end                                                            as arrival_source,

    -- delay from TCAT's record only (unchanged meaning, for comparison)
    j.delay_seconds                                                as tcat_delay_seconds,

    -- delay from the best available arrival. Positive = late.
    timestamp_diff(
        coalesce(j.actual_arrival_at,
                 timestamp_sub(j.gps_arrived_at, interval c.gps_lag_seconds second)),
        j.scheduled_arrival_at, second
    )                                                              as delay_seconds,

    -- GPS-only delay on every trip GPS saw. Lets you compare trips TCAT
    -- recorded vs missed on the same yardstick (see analyses/).
    timestamp_diff(
        timestamp_sub(j.gps_arrived_at, interval c.gps_lag_seconds second),
        j.scheduled_arrival_at, second
    )                                                              as gps_delay_seconds

from joined j
cross join calibration c
