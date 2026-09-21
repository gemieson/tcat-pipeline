-- One row each time a bus reaches a new stop on a trip.
-- A bus reports every ~30s whether it's moving, parked or between stops.
-- Keeping only the reports where last_stop_name changes collapses all of
-- that into the moments that matter. Within one trip the direction is fixed,
-- so non-unique stop names (both sides of a road) don't cause false changes.
-- Arrival time is bounded by the ~30s report interval.

with positions as (
    select * from {{ ref('int_vehicle_positions_deduped') }}
    where last_stop_name is not null
),

marked as (
    select
        *,
        lag(last_stop_name) over (
            partition by vehicle_id, trip_id
            order by vehicle_reported_at
        ) as prev_stop_name
    from positions
),

arrivals as (
    select * from marked
    where last_stop_name is distinct from prev_stop_name   -- first row of a trip counts
)

select
    dt,
    date(vehicle_reported_at, 'America/New_York')           as service_date,
    route_id,
    trip_id,
    vehicle_id,
    direction,
    last_stop_name                                          as stop_name,
    prev_stop_name,
    vehicle_reported_at                                     as arrived_at,

    -- time since the previous stop on this trip (null for the first stop)
    timestamp_diff(
        vehicle_reported_at,
        lag(vehicle_reported_at) over (
            partition by vehicle_id, trip_id order by vehicle_reported_at
        ),
        second
    )                                                       as seconds_from_prev_stop,

    deviation,
    display_status,
    on_board,
    total_capacity,
    latitude,
    longitude,
    is_stale

from arrivals