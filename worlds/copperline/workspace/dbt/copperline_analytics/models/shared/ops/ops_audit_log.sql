{{
    config(
        materialized='incremental',
        unique_key='audit_key',
        incremental_strategy='delete+insert'
    )
}}

{#-
    One row per dbt invocation, from the run hooks.

    `on-run-start` writes an `open` row before the graph starts and `on-run-end`
    writes a `close` row after it finishes. This model pairs them.

    Why a hook and not a model: a run that dies partway through never reaches a
    model, and a run that leaves no trace is the one you most want a trace of.
    The open row is written before anything can fail, so an invocation with an
    open and no close is a run that died, and that is the row worth alerting on.

    Incremental, keyed on the invocation. A full refresh throws away every run
    before this one and there is no way to get them back — the hook table is
    appended to, not rebuilt. Do not full-refresh this model.
-#}

with events as (

    select *
    from {{ source('ops_run', 'dbt_run_events') }}

    {% if is_incremental() %}
    where event_at > (select coalesce(max(closed_at), max(opened_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}

),

paired as (

    select
        invocation_id,
        max(ds)                                                     as ds,
        max(target_name)                                            as target_name,
        max(dbt_version)                                            as dbt_version,
        min(event_at) filter (where event = 'open')                 as opened_at,
        max(event_at) filter (where event = 'close')                as closed_at,
        max(models_selected) filter (where event = 'open')          as models_in_graph,
        max(models_selected) filter (where event = 'close')         as nodes_run,
        max(models_ok) filter (where event = 'close')               as nodes_ok,
        max(models_error) filter (where event = 'close')            as nodes_error
    from events
    group by 1

)

select
    invocation_id                               as audit_key,
    invocation_id,
    ds,
    target_name,
    dbt_version,
    opened_at,
    closed_at,
    case
        when closed_at is not null
        then date_diff('second', opened_at, closed_at)
    end                                         as duration_seconds,
    models_in_graph,
    coalesce(nodes_run, 0)                      as nodes_run,
    coalesce(nodes_ok, 0)                       as nodes_ok,
    coalesce(nodes_error, 0)                    as nodes_error,
    closed_at is null                           as run_did_not_finish,
    coalesce(nodes_error, 0) > 0                as run_had_failures

from paired
