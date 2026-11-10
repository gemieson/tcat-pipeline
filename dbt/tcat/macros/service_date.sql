{#
  The TCAT service day starts at 3am Eastern: a 12:30am trip belongs to the
  day it started. Every model that assigns a service date must use this macro,
  so the departure-board side and the GPS side always agree and can be joined.
#}
{% macro service_date(ts) -%}
    date(datetime_sub(datetime({{ ts }}, 'America/New_York'), interval 3 hour))
{%- endmacro %}
