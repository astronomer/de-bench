{{
    config(
        materialized='view'
    )
}}

{#- One row per till transaction.

    Two era columns, both left as they arrived:

    * `event_time_utc` is NULL for every transaction before the November 2025
      cutover. The store stamped local time and there was no UTC column. Filling
      it in needs the store's timezone, which is a join.
    * `business_date` is the store's own trading day, closed at 23:05
      store-local. It is not the UTC date and the two disagree on about one
      transaction in twelve. docs/runbooks/pos-ingestion.md holds the rule.

    Voided transactions are kept and flagged, not dropped: the till reconciliation
    counts them and the sales models do not.
-#}

select
    pos_txn_id,
    store_id,
    register_id,
    order_id,
    customer_ref,
    tender_type,
    business_date,
    event_time_local,
    event_time_utc,
    received_at,
    cast(gross_cents as bigint)         as gross_cents,
    cast(discount_cents as bigint)      as discount_cents,
    cast(tax_cents as bigint)           as tax_cents,
    cast(net_cents as bigint)           as net_cents,
    coalesce(return_flag, false)        as is_return,
    coalesce(void_flag, false)          as is_void,
    loaded_at
from {{ source('store', 'pos_sales_header') }}
