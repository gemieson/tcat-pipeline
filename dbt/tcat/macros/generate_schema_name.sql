{#
  By default dbt names custom schemas "<target>_<custom>", which would give
  tcat_staging_tcat_marts. Use the custom schema name as-is instead, so
  +schema: tcat_marts lands in the tcat_marts dataset.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
