-- One row each time a bus reaches a new stop on a trip.
-- A bus reports every ~30s whether it's moving, parked or between stops.
-- Keeping only the reports where last_stop_name changes collapses all of
-- that into the moments that matter. Within one trip the direction is fixed,
-- so non-unique stop names (both sides of a road) don't cause false changes.
--
-- Trip IDs repeat every day (trip 1900 = the 7pm run, daily), so a trip
-- instance is vehicle + trip + SERVICE DAY. The service day starts at 3am
-- Eastern, so late-night runs belong to the day they started.
--
-- How well is the arrival time known? Only to within the time since the
-- bus's previous report. is_reliable_arrival is false when that gap is longer
-- than max_report_gap_seconds (collector down, GPS dropout) or when there is
-- no previous report (a trip's first row: the bus may have been sitting at
-- that stop for a while). The GPS fill uses reliable arrivals only.

with positions as (
    select
        *,
        {{ service_date('vehicle_reported_at') }} as service_date
    from {{ ref('int_vehicle_positions_deduped') }}
    where last_stop_name is not null
),

marked as (
    select
        *,
        lag(last_stop_name)      over trip_order as prev_stop_name,
        lag(vehicle_reported_at) over trip_order as prev_reported_at
    from positions
    window trip_order as (
        partition by vehicle_id, trip_id, service_date
        order by vehicle_reported_at
    )
),

arrivals as (
    select * from marked
    where last_stop_name is distinct from prev_stop_name   -- first row of a trip counts
)

select
    dt,
    service_date,
    route_id,
    trip_id,
    vehicle_id,
    direction,
    last_stop_name                                          as stop_name,
    prev_stop_name,
    vehicle_reported_at                                     as arrived_at,

    -- time since the previous stop on this trip instance (null for the first stop)
    timestamp_diff(
        vehicle_reported_at,
        lag(vehicle_reported_at) over (
            partition by vehicle_id, trip_id, service_date
            order by vehicle_reported_at
        ),
        second
    )                                                       as seconds_from_prev_stop,

    -- time since this bus's previous report of any kind (null for the first)
    timestamp_diff(vehicle_reported_at, prev_reported_at, second)
                                                            as seconds_since_prev_report,
    coalesce(
        timestamp_diff(vehicle_reported_at, prev_reported_at, second)
            <= {{ var('max_report_gap_seconds', 120) }},
        false
    )                                                       as is_reliable_arrival,

    deviation,
    display_status,
    on_board,
    total_capacity,
    latitude,
    longitude,
    is_stale

from arrivals
