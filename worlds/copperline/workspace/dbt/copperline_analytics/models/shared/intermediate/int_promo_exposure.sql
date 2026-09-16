{{
    config(
        materialized='table'
    )
}}

{#-
    Eligible against actually discounted.

    A promotion's performance is not "how much did we give away". It is "how
    many orders could have taken it, and how many did". Without the eligible
    side, a promotion that nobody could use and a promotion that nobody wanted
    look identical, and both look like a saving.

    Eligibility is three conditions, all of them from the promotion master:

    * the order date sits inside the promotion window,
    * the order's market is in the promotion's market list,
    * the order is worth at least `min_order_cents`.

    That is what the promotion says about itself. It is not the whole truth — a
    `sku_list` promotion also needs a line carrying one of its SKUs, and the
    master does not carry the list — so `eligibility_is_complete` says which
    kind of check was possible. A model that reports take-up on `sku_list` and
    `category` promotions without saying so is reporting a ceiling, not a rate.

    The window is the trailing 400 days from `var('ds')`, matching
    docs/retention-policy.md RET-2. Every eligible order against every live
    promotion is a wide cross product and it is not worth carrying the whole
    history of it; the promotions that closed more than a year ago are read off
    the order lines, not off take-up.

    Growth reads this for promotion performance and customer reads it for
    offer take-up by segment. One model, one definition of eligible.
-#}

with promos as (

    select
        promo_id,
        promo_code,
        promo_type,
        applies_to,
        funded_by,
        stackable,
        value_bps,
        value_cents,
        coalesce(min_order_cents, 0) as min_order_cents,
        starts_on,
        ends_on,
        market_code_list
    from {{ ref('stg_sales__promotions') }}

),

orders as (

    select
        order_id,
        order_date,
        channel,
        market_code,
        customer_ref,
        loyalty_id,
        booked_cents
    from {{ ref('int_orders_enriched') }}
    where order_date > {{ ds_minus(400) }}

),

applied as (

    select
        order_id,
        promo_id,
        sum(discount_cents)                                     as discount_cents,
        count(*)                                                as application_count,
        count(*) filter (where is_header_discount)              as header_applications,
        count(*) filter (where not is_header_discount)          as line_applications
    from {{ ref('stg_sales__promo_applications') }}
    group by 1, 2

),

eligible as (

    select
        p.promo_id,
        p.promo_code,
        p.promo_type,
        p.applies_to,
        p.funded_by,
        p.stackable,
        o.order_id,
        o.order_date,
        o.channel,
        o.market_code,
        o.customer_ref,
        o.loyalty_id,
        o.booked_cents,
        p.min_order_cents
    from promos p
    join orders o
      on o.order_date between p.starts_on and p.ends_on
     and list_contains(p.market_code_list, o.market_code)
     and o.booked_cents >= p.min_order_cents

)

select
    {{ dbt_utils.surrogate_key(['e.promo_id', 'e.order_id']) }} as promo_exposure_key,
    e.promo_id,
    e.promo_code,
    e.promo_type,
    e.applies_to,
    e.funded_by,
    e.stackable,
    e.order_id,
    e.order_date,
    e.channel,
    e.market_code,
    e.customer_ref,
    e.loyalty_id,
    e.booked_cents,
    e.min_order_cents,

    true                                            as is_eligible,
    a.promo_id is not null                          as is_discounted,
    coalesce(a.discount_cents, 0)                   as discount_cents,
    coalesce(a.application_count, 0)                as application_count,
    coalesce(a.header_applications, 0)              as header_applications,
    coalesce(a.line_applications, 0)                as line_applications,

    -- An `order` promotion is fully checkable from the master. The other two
    -- kinds need a list the master does not carry, so the eligible side is a
    -- ceiling and the take-up rate read off it is a floor.
    e.applies_to = 'order'                          as eligibility_is_complete

from eligible e
left join applied a on a.order_id = e.order_id and a.promo_id = e.promo_id
