{{ config(materialized='table') }}

{#-
    Marketplace GMV by day and seller. contracts/alert_subjects.yml watches it.

    `gmv_cents` is the full marketplace order value, sellers included — a
    reserved name and the one people most often compute as "our share", which is
    `commission_cents`. Both are here so nobody has to choose the wrong one.

    There is no clickstream from the marketplace and there never will be. A
    conversion rate that divides marketplace orders by web sessions is wrong,
    and this table carries no session column so that nobody can write it by
    accident.
-#}

select
    s.placed_date                               as date_key,
    s.placed_date                               as ds,
    s.seller_id,
    s.currency_code                             as currency_key,
    s.currency_code,

    count(*)                                    as order_count,
    count(*) filter (where s.order_state = 'shipped')   as shipped_order_count,
    count(*) filter (where s.order_state = 'refunded')  as refunded_order_count,
    count(*) filter (where s.order_state = 'cancelled') as cancelled_order_count,

    sum(s.gmv_cents)                            as gmv_cents,
    sum(s.commission_cents)                     as commission_cents,
    sum(s.fulfilment_fee_cents)                 as fulfilment_fee_cents,
    sum(s.refund_cents)                         as refund_cents,
    sum(s.principal_cents)                      as principal_cents,
    sum(s.seller_net_cents)                     as seller_net_cents,

    sum(s.commission_rate_variance_cents)       as commission_rate_variance_cents,
    count(*) filter (where not s.is_settled)    as unsettled_order_count,
    count(*) filter (where s.has_refund)        as orders_with_refund,

    cast(round(
        cast(sum(s.commission_cents) as decimal(38, 4)) * 10000
        / nullif(sum(s.gmv_cents), 0), 0
    ) as integer)                               as effective_take_rate_bps

from {{ ref('int_settlement_matched') }} s
group by 1, 2, 3, 4, 5
