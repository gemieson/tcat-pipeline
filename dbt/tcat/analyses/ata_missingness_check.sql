-- Are the trips TCAT fails to record different from the ones it records?
-- If missing-ATA trips were systematically later, delay figures built on ATA
-- alone would be too optimistic. Compare both groups on the same yardstick:
-- the calibrated GPS delay, which exists for both.
--
-- Run: dbt compile --select ata_missingness_check, then paste
-- target/compiled/tcat/analyses/ata_missingness_check.sql into BigQuery.
--
-- Sep 18-21 sample (laptop data): ATA recorded -> median 3.4 min late, 55% on
-- time (n=2,316); ATA missing -> median 3.6 min, 60% on time (n=764).
-- No sign of bias in that sample; re-check on the full window.

select
    if(has_tcat_actual, 'ATA recorded', 'ATA missing')                    as tcat_record,
    count(*)                                                              as trips_with_gps,
    round(approx_quantiles(gps_delay_seconds, 2)[safe_offset(1)] / 60, 1) as median_gps_delay_min,
    round(avg(gps_delay_seconds) / 60, 1)                                 as avg_gps_delay_min,
    round(100 * countif(gps_delay_seconds between -60 and 300) / count(*), 1)
                                                                          as pct_on_time_gps
from {{ ref('fct_stop_arrivals') }}
where has_gps_arrival
group by 1
order by 1
