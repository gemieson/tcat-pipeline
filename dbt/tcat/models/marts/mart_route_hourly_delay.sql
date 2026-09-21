-- The headline answer: how late is each route, by hour of day (Ithaca time)?
-- One row per route per hour across the whole analysis window.
-- measured_trips shows how much evidence sits behind each average; treat
-- rows with only a handful of measured trips with caution.

select
    route_id,
    hour_eastern,

    count(*)                                                    as scheduled_trips,
    countif(has_tcat_actual)                                    as measured_trips,
    round(100 * countif(has_tcat_actual) / count(*), 1)         as pct_measured,

    round(avg(delay_seconds) / 60, 1)                           as avg_delay_min,
    round(approx_quantiles(delay_seconds, 2)[safe_offset(1)] / 60, 1)
                                                                as median_delay_min,
    round(100 * countif(is_on_time)
          / nullif(countif(has_tcat_actual), 0), 1)             as pct_on_time,

    round(avg(abs(prediction_error_seconds)) / 60, 1)           as avg_abs_prediction_error_min

from {{ ref('fct_stop_arrivals') }}
group by route_id, hour_eastern
