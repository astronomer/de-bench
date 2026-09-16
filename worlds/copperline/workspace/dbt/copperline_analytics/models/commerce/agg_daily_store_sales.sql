{{ config(materialized='table') }}

{#-
    Store sales by day, from the order spine, with the till totals beside them.

    The two do not agree and this model does not pick one. The order book is the
    company's record of what sold; the POS batch is the store's record of what
    the till took. They differ because a store transaction can be voided after
    the batch closed, because the store's trading day ends at 23:05 store-local
    and the order's date is the local order date, and because the pre-cutover
    era has no UTC stamp at all to line them up with.

    `variance_cents` is the gap. marts.recon_exceptions is where it is worked.
-#}

with orders as (

    select
        order_date                              as ds,
        store_id,
        count(*)                                as order_count,
        sum(booked_cents)                       as booked_cents,
        sum(order_discount_cents)               as order_discount_cents,
        sum(tax_cents)                          as tax_cents,
        sum(net_sales_cents)                    as net_sales_cents,
        sum(returned_cents)                     as returned_cents
    from {{ ref('fct_order') }}
    where channel = 'store'
      and store_id is not null
    group by 1, 2

),

till as (

    select
        business_date                           as ds,
        store_id,
        count(*)                                as txn_count,
        count(*) filter (where is_void)         as void_count,
        count(*) filter (where is_return)       as till_return_count,
        sum(gross_cents) filter (where not is_void)     as till_gross_cents,
        sum(discount_cents) filter (where not is_void)  as till_discount_cents,
        sum(tax_cents) filter (where not is_void)       as till_tax_cents,
        sum(net_cents) filter (where not is_void)       as till_net_cents
    from {{ ref('stg_store__pos_sales_header') }}
    group by 1, 2

),

manifest as (

    select
        business_date                           as ds,
        store_id,
        count(*)                                as batch_count,
        count(*) filter (where status = 'late')    as late_batches,
        count(*) filter (where status = 'missing') as missing_batches,
        sum(missing_row_count)                  as claimed_minus_received_rows
    from {{ ref('stg_store__pos_batch_manifest') }}
    group by 1, 2

),

joined as (

    select ds, store_id from orders
    union
    select ds, store_id from till

)

select
    j.ds                                        as date_key,
    j.ds,
    j.store_id                                  as store_key,
    j.store_id,
    s.region_code,
    s.market_code                               as geography_key,
    s.store_format,
    s.source_book,

    coalesce(o.order_count, 0)                  as order_count,
    coalesce(o.booked_cents, 0)                 as booked_cents,
    coalesce(o.order_discount_cents, 0)         as order_discount_cents,
    coalesce(o.tax_cents, 0)                    as tax_cents,
    coalesce(o.returned_cents, 0)               as returned_cents,
    coalesce(o.net_sales_cents, 0)              as net_sales_cents,

    coalesce(t.txn_count, 0)                    as till_txn_count,
    coalesce(t.void_count, 0)                   as till_void_count,
    coalesce(t.till_return_count, 0)            as till_return_count,
    coalesce(t.till_gross_cents, 0)             as till_gross_cents,
    coalesce(t.till_discount_cents, 0)          as till_discount_cents,
    coalesce(t.till_net_cents, 0)               as till_net_cents,

    coalesce(m.batch_count, 0)                  as batch_count,
    coalesce(m.late_batches, 0)                 as late_batches,
    coalesce(m.missing_batches, 0)              as missing_batches,
    coalesce(m.claimed_minus_received_rows, 0)  as claimed_minus_received_rows,

    coalesce(t.till_net_cents, 0) - (coalesce(o.booked_cents, 0) - coalesce(o.order_discount_cents, 0))
                                                as variance_cents,
    o.ds is null                                as missing_from_order_book,
    t.ds is null                                as missing_from_till

from joined j
left join orders o on o.ds = j.ds and o.store_id = j.store_id
left join till t on t.ds = j.ds and t.store_id = j.store_id
left join manifest m on m.ds = j.ds and m.store_id = j.store_id
left join {{ ref('dim_store') }} s on s.store_id = j.store_id
