{{ config(materialized='table') }}

{#-
    Promotion performance: eligible, taken, and what it cost.

    Take-up is `orders_discounted / orders_eligible`, and both sides come from
    the shared int_promo_exposure so that customer's offer report and growth's
    promotion report divide by the same thing.

    `eligibility_is_complete` is false for `sku_list` and `category`
    promotions, whose eligible side is a ceiling rather than a count. The
    take-up rate on those rows is a floor. It is on the row rather than in a
    footnote because the footnote never travels with the number.
-#}

select
    e.promo_id,
    e.promo_code,
    e.promo_type,
    e.applies_to,
    e.funded_by,
    e.market_code                               as geography_key,
    e.channel                                   as channel_key,

    min(e.order_date)                           as first_eligible_date,
    max(e.order_date)                           as last_eligible_date,

    count(*)                                    as orders_eligible,
    count(*) filter (where e.is_discounted)     as orders_discounted,
    sum(e.discount_cents)                       as discount_cents,
    sum(e.booked_cents)                         as eligible_booked_cents,
    sum(e.booked_cents) filter (where e.is_discounted) as discounted_booked_cents,
    sum(e.header_applications)                  as header_applications,
    sum(e.line_applications)                    as line_applications,

    cast(round(
        cast(count(*) filter (where e.is_discounted) as decimal(38, 4)) * 10000
        / nullif(count(*), 0), 0
    ) as integer)                               as take_up_rate_bps,

    cast(round(
        cast(sum(e.discount_cents) as decimal(38, 4)) * 10000
        / nullif(sum(e.booked_cents) filter (where e.is_discounted), 0), 0
    ) as integer)                               as discount_depth_bps,

    bool_and(e.eligibility_is_complete)         as eligibility_is_complete

from {{ ref('int_promo_exposure') }} e
group by 1, 2, 3, 4, 5, 6, 7
