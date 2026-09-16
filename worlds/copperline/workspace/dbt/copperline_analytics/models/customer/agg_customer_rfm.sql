{{ config(materialized='table') }}

{#-
    Recency, frequency and value, scored one to five.

    **Derived here, not read from source.** The upstream model has no
    pre-computed segment table on purpose, so a segment is always somebody's
    definition and this is the customer team's. Growth has its own in
    marts.audience_segments and the two do not have to agree; they have to say
    whose they are, which is what the model path does.

    Scores are quintiles within the population, not fixed thresholds. A fixed
    threshold ages: the FY2024 version called anybody over fifty thousand cents
    "high value" and by FY2026 that was two thirds of the book.

    The denominator is trade accounts. Guests and loyalty-only members are not
    scored, because a score on an identity that changes every visit is noise.
    int_customer_lifecycle carries the guest denominator if anybody needs it.
-#}

with base as (

    select
        l.party_key                             as customer_id,
        l.account_name,
        l.region_code,
        l.tier,
        l.days_since_last_order,
        l.orders_12m,
        l.orders_lifetime,
        l.net_sales_cents_12m,
        l.net_sales_cents_lifetime,
        l.average_order_net_cents,
        l.first_order_date,
        l.last_order_date,
        l.is_churned,
        l.is_new
    from {{ ref('int_customer_lifecycle') }} l
    where l.party_kind = 'trade'
      and l.orders_lifetime > 0

),

scored as (

    select
        *,
        6 - ntile(5) over (order by days_since_last_order)  as recency_score,
        ntile(5) over (order by orders_12m)                 as frequency_score,
        ntile(5) over (order by net_sales_cents_12m)        as value_score
    from base

)

select
    {{ ds() }}                                  as ds,
    customer_id,
    account_name,
    coalesce(region_code, 'unknown')            as region,
    tier,

    days_since_last_order,
    orders_12m,
    orders_lifetime,
    net_sales_cents_12m,
    net_sales_cents_lifetime                    as net_sales_cents,
    average_order_net_cents,
    first_order_date,
    last_order_date,

    recency_score,
    frequency_score,
    value_score,
    cast(recency_score as varchar) || cast(frequency_score as varchar)
        || cast(value_score as varchar)         as rfm_cell,
    recency_score + frequency_score + value_score as rfm_total,

    case
        when recency_score >= 4 and frequency_score >= 4 and value_score >= 4 then 'champion'
        when recency_score >= 4 and frequency_score >= 3                      then 'loyal'
        when recency_score >= 4                                              then 'promising'
        when recency_score <= 2 and value_score >= 4                         then 'at risk, valuable'
        when recency_score <= 2                                              then 'lapsing'
        else 'steady'
    end                                         as rfm_segment,

    is_churned,
    is_new

from scored
