{#
    Custom schema names are used verbatim.

    dbt's default prefixes the target schema onto a custom one, so a model
    configured into `marts` lands in `main_marts`. Every consumer outside this
    project — the dashboards, the exports, the reverse-ETL readers, the two
    name-pattern sensors — names its tables `marts.<model>` as a string. Prefix
    the schema and every one of them breaks, and none of them breaks loudly.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}


{#
    The pinned warehouse clock.

    Read this, never current_date and never a Python clock. A model that reads
    the wall clock returns one answer on the night it runs and another on the
    replay, and every backfill this team has run is a replay. plat_backfill_broker
    passes the date it is replaying as --vars '{ds: ...}'.
#}
{% macro ds() -%}
    date '{{ var("ds") }}'
{%- endmacro %}


{#
    ds minus n days, as a date literal, for the rolling windows.
#}
{% macro ds_minus(days) -%}
    (date '{{ var("ds") }}' - interval '{{ days }} days')::date
{%- endmacro %}
