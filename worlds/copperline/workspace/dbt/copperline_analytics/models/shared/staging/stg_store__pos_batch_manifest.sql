{{
    config(
        materialized='view'
    )
}}

{#- One row per store-night file.

    `claimed_row_count` is what the store's till said it sent; `row_count` is
    what arrived. They disagree on a small number of nights and that gap is the
    only evidence a POS file was truncated in transit.
-#}

select
    batch_id,
    store_id,
    business_date,
    file_name,
    status,
    cast(claimed_row_count as integer)                          as claimed_row_count,
    cast(row_count as integer)                                  as row_count,
    cast(claimed_row_count as integer) - cast(row_count as integer) as missing_row_count,
    stamped_at,
    received_at
from {{ source('store', 'pos_batch_manifest') }}
