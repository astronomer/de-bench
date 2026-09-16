{{
    config(
        materialized='view'
    )
}}

{#- The pre-history slab: store-day totals for the era before the header feed.

    This table and stg_store__pos_sales_header do not overlap. The slab ends the
    day the header feed starts. A model that unions them has to respect the
    boundary or it double-counts one day; a model that reads only one of them is
    missing an era.
-#}

select
    store_id,
    business_date,
    cast(txn_count as integer)          as txn_count,
    cast(gross_cents as bigint)         as gross_cents,
    cast(discount_cents as bigint)      as discount_cents,
    cast(tax_cents as bigint)           as tax_cents,
    cast(net_cents as bigint)           as net_cents
from {{ source('store', 'pos_sales_daily') }}
