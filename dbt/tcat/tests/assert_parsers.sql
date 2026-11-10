-- The two TCAT parsers on constant inputs. Reads no table, so it is free.
-- Returns a row for every case a parser gets wrong.
--   /Date(ms-0400)/: the digits are UTC milliseconds; applying the -0400
--   offset would shift every timestamp by 4 hours.
with dates as (
    select '/Date(1789944938000-0400)/' as raw, timestamp '2026-09-20 22:55:38+00' as expected union all
    select '/Date(1789944938000)/',             timestamp '2026-09-20 22:55:38+00' union all
    select '/Date(-1000+0000)/',                timestamp '1969-12-31 23:59:59+00' union all
    select 'not a date',                        cast(null as timestamp)           union all
    select cast(null as string),                cast(null as timestamp)
),

devs as (
    select '00:02:48' as raw, 168 as expected union all
    select '-00:01:30',       -90             union all
    select '01:00:00',        3600            union all
    select 'garbage',         cast(null as int64) union all
    select cast(null as string), cast(null as int64)
)

select 'dotnet_timestamp' as parser, raw, cast(expected as string) as expected,
       cast({{ dotnet_timestamp('raw') }} as string) as got
from dates
where {{ dotnet_timestamp('raw') }} is distinct from expected

union all

select 'dev_seconds', raw, cast(expected as string), cast({{ dev_seconds('raw') }} as string)
from devs
where {{ dev_seconds('raw') }} is distinct from expected
