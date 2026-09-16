{{ config(materialized='table') }}

{#-
    Marketplace settlement by fiscal week and seller. contracts/settlement_weekly.yml.

    The week is the **fiscal** week, not the ISO week. Copperline runs 4-5-4 and
    FY2023 has a fifty-third one; a settlement summary cut on ISO weeks drifts
    from the finance close by a day and then by a week, which is what happened
    in FY2024 P11 and cost a seller three weeks of chasing.

    **`commission_cents` and `fee_cents` are what we held back.** SS-1 makes the
    payout the order value less both of them, so the two together have to come
    to what Copperline actually kept out of the seller's orders. The settlement
    feed signs its own lines and `int_settlement_matched` pivots them on the
    magnitude, which strips the direction off — so the direction is stated here.
    The commission and the fulfilment fee are ours and add; a refund hands part
    of the commission back to the seller and comes off.
-#}

with settled as (

    select * from {{ ref('int_settlement_matched') }}

),

weeks as (

    select cal_date, fiscal_week_start, fiscal_year, fiscal_week, fiscal_month
    from {{ ref('dim_date') }}

)

select
    w.fiscal_week_start                         as week_start,
    s.seller_id,
    w.fiscal_year,
    w.fiscal_week,
    w.fiscal_month,

    count(*)                                    as order_count,
    sum(s.gmv_cents)                            as gmv_cents,
    sum(s.commission_cents)                     as commission_cents,
    sum(s.fulfilment_fee_cents - s.refund_cents) as fee_cents,
    sum(s.seller_net_cents)                     as payout_cents,

    count(*) filter (where s.is_fully_settled)  as fully_settled_orders,
    count(*) filter (where not s.is_settled)    as unsettled_orders,
    count(distinct s.payout_id)                 as payout_count,
    min(s.settled_from_date)                    as first_settled_date,
    max(s.settled_to_date)                      as last_settled_date

from settled s
join weeks w on w.cal_date = s.placed_date
group by 1, 2, 3, 4, 5
