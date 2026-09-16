{{
    config(
        materialized='view'
    )
}}

{#- Which processor is authoritative, and between which dates.

    One row per processor. `authoritative_to` is NULL on the processor that is
    still running.

    The switchover date is on no settlement row on either feed, so this table is
    the only place it is written as data — docs/runbooks/processor-migration.md
    PRM-2, which also says why it must be read rather than typed: the boundary is
    a business decision, it was taken twice during the migration and it moved
    once.
-#}

select
    processor,
    authoritative_from,
    authoritative_to,
    authoritative_to is null            as is_current
from {{ source('payments', 'pay_processor_windows') }}
