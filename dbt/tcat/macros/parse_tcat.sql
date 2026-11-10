{#
  Parsers for TCAT's two odd formats. Kept as macros so both staging models
  share one definition, and tests/assert_parsers.sql can check them on
  constant inputs without scanning any data.
#}

{# .NET JSON date, e.g. /Date(1789944938000-0400)/. The digits are already
   UTC milliseconds; the offset is display metadata, and adding it is a silent
   4-hour error. #}
{% macro dotnet_timestamp(col) -%}
    timestamp_millis(safe_cast(regexp_extract({{ col }}, r'/Date\((-?\d+)') as int64))
{%- endmacro %}

{# Dev, e.g. "00:02:48" -> 168. Handles a leading minus. Unparseable -> null
   rather than failing the whole model. #}
{% macro dev_seconds(col) -%}
    case
        when {{ col }} is null then null
        else (case when starts_with({{ col }}, '-') then -1 else 1 end) * (
              safe_cast(split(ltrim({{ col }}, '-'), ':')[safe_offset(0)] as int64) * 3600
            + safe_cast(split(ltrim({{ col }}, '-'), ':')[safe_offset(1)] as int64) * 60
            + safe_cast(split(ltrim({{ col }}, '-'), ':')[safe_offset(2)] as int64)
        )
    end
{%- endmacro %}
