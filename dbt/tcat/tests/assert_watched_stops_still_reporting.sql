{{ config(severity='warn') }}
-- A watched stop that had trips on this weekday last week but none yesterday.
-- Usually TCAT renumbered the stop or collection was down, not a schedule
-- change; either way the delay figures for that stop quietly stop updating.
with days as (
    select date_sub(current_date('America/New_York'), interval 1 day) as yesterday
),

last_week as (
    select distinct stop_id from {{ ref('fct_stop_arrivals') }}, days
    where service_date = date_sub(yesterday, interval 7 day)
),

recent as (
    select distinct stop_id from {{ ref('fct_stop_arrivals') }}, days
    where service_date = yesterday
)

select l.stop_id
from last_week l
left join recent r using (stop_id)
where r.stop_id is null
