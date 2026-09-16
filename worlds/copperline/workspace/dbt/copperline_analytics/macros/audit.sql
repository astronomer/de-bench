{#
    The run audit.

    on-run-start opens a row for the invocation before any model builds.
    on-run-end closes it with the model count and the outcome. ops_audit_log is
    the incremental model over the rows these two hooks write.

    Why a hook and not a model: the row has to exist before the graph starts, so
    that a run which dies partway through still leaves a trace. A model cannot
    record a run that never reached it.
#}

{% macro copperline_audit_open() %}
    {% if execute %}
        {% set sql %}
            create schema if not exists ops_run;

            create table if not exists ops_run.dbt_run_events (
                invocation_id   varchar,
                event           varchar,
                event_at        timestamp,
                ds              date,
                target_name     varchar,
                dbt_version     varchar,
                models_selected integer,
                models_ok       integer,
                models_error    integer
            );

            insert into ops_run.dbt_run_events values (
                '{{ invocation_id }}',
                'open',
                now(),
                date '{{ var("ds") }}',
                '{{ target.name }}',
                '{{ dbt_version }}',
                {{ graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list | length }},
                null,
                null
            );
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}


{% macro copperline_audit_close(results) %}
    {% if execute %}
        {% set ok = results | selectattr("status", "in", ["success", "pass"]) | list | length %}
        {% set bad = results | rejectattr("status", "in", ["success", "pass"]) | list | length %}
        {% set sql %}
            insert into ops_run.dbt_run_events values (
                '{{ invocation_id }}',
                'close',
                now(),
                date '{{ var("ds") }}',
                '{{ target.name }}',
                '{{ dbt_version }}',
                {{ results | length }},
                {{ ok }},
                {{ bad }}
            );
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
