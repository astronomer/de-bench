{{
    config(
        materialized='table'
    )
}}

{#-
    The promotion master, with what it actually did attached.

    A promotion that ran and a promotion that was set up and never fired look
    the same in the source. `orders_discounted` and `discount_cents` come from
    int_promo_exposure and tell them apart, which is the one question anybody
    asks of this dimension.
-#}

with promos as (

    select * from {{ ref('stg_sales__promotions') }}

),

take_up as (

    select
        promo_id,
        count(*)                                        as orders_eligible,
        count(*) filter (where is_discounted)           as orders_discounted,
        sum(discount_cents)                             as discount_cents,
        min(order_date) filter (where is_discounted)    as first_used_on,
        max(order_date) filter (where is_discounted)    as last_used_on,
        bool_and(eligibility_is_complete)               as eligibility_is_complete
    from {{ ref('int_promo_exposure') }}
    group by 1

)

select
    p.promo_id                                  as promotion_key,
    p.promo_id,
    p.promo_code,
    p.promo_type,
    p.applies_to,
    p.funded_by,
    p.stackable,
    p.stack_priority,
    p.value_bps,
    p.value_cents,
    p.min_order_cents,
    p.market_codes,
    p.market_code_list,
    p.starts_on,
    p.ends_on,
    date_diff('day', p.starts_on, p.ends_on) + 1        as window_days,
    p.starts_on <= {{ ds() }} and p.ends_on >= {{ ds() }} as is_live,

    coalesce(t.orders_eligible, 0)              as orders_eligible,
    coalesce(t.orders_discounted, 0)            as orders_discounted,
    coalesce(t.discount_cents, 0)               as discount_cents,
    t.first_used_on,
    t.last_used_on,
    coalesce(t.orders_discounted, 0) > 0        as was_used,

    -- A ceiling for sku_list and category promotions: the master does not carry
    -- the list, so the eligible side is everything in the window.
    coalesce(t.eligibility_is_complete, false)  as eligibility_is_complete

from promos p
left join take_up t on t.promo_id = p.promo_id
