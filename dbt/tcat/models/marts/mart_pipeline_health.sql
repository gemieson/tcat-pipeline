-- One row per UTC hour per stream, from the first hour of the producer's
-- request log onward: is collection complete, and did everything the
-- producer delivered to Kafka reach BigQuery? (docs/DECISIONS.md, D12)
--
--   expected_requests   polls the producer should have made that hour
--   requests            polls it logged. An hour with none still gets a row,
--                       from the hour spine, so a dead producer shows as 0.
--   delivered           messages the broker confirmed
--   staged              rows in staging for that hour, after replay removal
--   missing_in_bigquery delivered - staged. Should be 0: the producer is
--                       idempotent and staging drops replays, so any gap is
--                       data lost between Kafka and GCS.
--
-- Hours are assigned by the timestamp the envelopes carry (ingested_at), on
-- both sides, so a poll straddling an hour boundary isn't split.
--
-- health_now exists so the unit test can fix the clock; leave it unset.
{%- set now = var('health_now', 'current_timestamp()') %}

with log as (
    select
        source                                                       as stream,
        timestamp_trunc(coalesce(envelope_ingested_at, requested_at), hour) as hour,
        http_code,
        error,
        records,
        produced,
        delivered,
        failed,
        skipped
    from {{ ref('stg_request_log') }}
    where writer = 'producer'
),

spine as (
    select hour, stream
    from unnest(generate_timestamp_array(
             (select min(hour) from log),
             timestamp_trunc({{ now }}, hour),
             interval 1 hour)) as hour
    cross join unnest(['vehicles', 'departures']) as stream
),

requests as (
    select
        stream,
        hour,
        count(*)                                         as requests,
        countif(http_code = 200 and error is null)       as requests_ok,
        sum(records)                                     as records_in_responses,
        sum(produced)                                    as produced,
        sum(delivered)                                   as delivered,
        sum(failed)                                      as failed,
        sum(skipped)                                     as skipped
    from log
    group by stream, hour
),

staged as (
    select 'vehicles' as stream, timestamp_trunc(ingested_at, hour) as hour, count(*) as staged
    from {{ ref('stg_vehicle_positions') }}
    group by 1, 2
    union all
    select 'departures', timestamp_trunc(ingested_at, hour), count(*)
    from {{ ref('stg_stop_departures') }}
    group by 1, 2
)

select
    s.hour,
    s.stream,
    case s.stream
        when 'vehicles'   then div(3600, {{ var('vehicle_poll_seconds', 30) }})
        when 'departures' then div(3600, {{ var('departure_poll_seconds', 60) }})
                               * (select count(*) from {{ ref('watched_stops') }})
    end                                                  as expected_requests,
    coalesce(r.requests, 0)                              as requests,
    coalesce(r.requests_ok, 0)                           as requests_ok,
    coalesce(r.records_in_responses, 0)                  as records_in_responses,
    coalesce(r.produced, 0)                              as produced,
    coalesce(r.delivered, 0)                             as delivered,
    coalesce(r.failed, 0)                                as failed,
    coalesce(r.skipped, 0)                               as skipped,
    coalesce(st.staged, 0)                               as staged,
    coalesce(r.delivered, 0) - coalesce(st.staged, 0)    as missing_in_bigquery,

    -- Judge only complete hours: skip the first (the log started mid-hour)
    -- and the last 3 (consumer flushes, the hourly sync, the daily build).
    s.hour > (select min(hour) from log)
      and s.hour < timestamp_sub({{ now }}, interval 3 hour)
                                                         as is_checkable
from spine s
left join requests r using (stream, hour)
left join staged st using (stream, hour)
