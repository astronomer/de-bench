{{
    config(
        materialized='view'
    )
}}

{#- One row per ingestion job per business date.

    `history_complete` says whether the job has ever backfilled to the start of
    its range. A false here is why a mart looks thin in an early period and is
    not a modelling defect.

    Thirteen of the job names in this table belong to jobs that still run under
    the old orchestrator — docs/runbooks/orchestrator-migration.md lists which
    moved and when.
-#}

select
    cast(load_id as bigint)             as load_id,
    job_name,
    stream_name,
    business_date,
    staging_schema,
    watermark_column,
    watermark_value,
    load_status,
    cast(rows_loaded as bigint)         as rows_loaded,
    coalesce(history_complete, false)   as history_complete,
    started_at,
    ended_at,
    case
        when ended_at is not null
        then date_diff('second', started_at, ended_at)
    end                                 as duration_seconds
from {{ source('ops', 'load_control') }}
