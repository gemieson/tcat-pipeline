-- The marts are current, not just the raw files. Source freshness only proves
-- that files keep arriving; this catches the case where they do, but a
-- parsing or filtering change silently empties everything downstream.
select max(service_date) as newest_service_date
from {{ ref('fct_stop_arrivals') }}
having coalesce(max(service_date), date '1900-01-01')
       < date_sub(current_date('America/New_York'), interval 2 day)
