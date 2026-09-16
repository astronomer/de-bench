{{
    config(
        materialized='table'
    )
}}

{#-
    First order, last order, tenure, churn — and the denominator.

    **The denominator is the point of this model.** About a fifth of orders have
    no customer at all: the store and web channels take guest checkout and the
    OMS writes neither a trade account nor a loyalty id. Every per-customer
    metric in the warehouse therefore has to say what it is dividing by, and
    before this model existed each mart chose for itself. Average order value
    was three different numbers depending on which mart you asked, and the
    difference was entirely whether guests were in the bottom of the fraction.

    So the grain here is the **party**, and a party is one of three things:

    * `trade`   — a trade account, resolved through int_customer_resolved.
    * `loyalty` — a loyalty member with no trade account. Most consumer orders.
    * `guest`   — no identity at all. One row per market, holding the orders and
                  the money that belong to nobody. Divide by these or exclude
                  them, but say which.

    **A guest row is a bucket and not a person.** It holds the counts and the
    money, which is what it is for, and it is empty on every column that
    describes one customer's history: the two dates, the two durations and the
    three flags. Empty rather than false — false is a claim about a customer and
    there is no customer on the row. Left as it was, the bucket took an order in
    every market every day, so it never aged and never churned, and it sorted to
    the top of the book as the oldest and busiest party we had.

    Churn is stated, not inferred: no order in the 180 days ending on
    `var('ds')`. That threshold is a business definition and it is written down
    in docs/semantic-definitions.md §SD-3; it is not a number anyone should be
    picking inside a mart. SD-3 carries the guest rule above as well.
-#}

with orders as (

    select
        order_id,
        order_date,
        channel,
        market_code,
        customer_ref,
        loyalty_id,
        booked_cents,
        net_sales_cents,
        returned_cents,
        net_units
    from {{ ref('int_orders_enriched') }}

),

resolved as (

    select party_ref, customer_key, source_book, account_name, market_code, region_code, tier, status
    from {{ ref('int_customer_resolved') }}

),

keyed as (

    select
        o.*,
        case
            when o.customer_ref is not null then coalesce(r.customer_key, o.customer_ref)
            when o.loyalty_id is not null   then o.loyalty_id
            else 'GUEST-' || o.market_code
        end as party_key,
        case
            when o.customer_ref is not null then 'trade'
            when o.loyalty_id is not null   then 'loyalty'
            else 'guest'
        end as party_kind,
        r.account_name,
        r.source_book,
        r.region_code,
        r.tier
    from orders o
    left join resolved r on r.party_ref = o.customer_ref

),

lifecycle as (

    select
        party_key,
        party_kind,
        max(market_code)                        as market_code,
        max(account_name)                       as account_name,
        max(source_book)                        as source_book,
        max(region_code)                        as region_code,
        max(tier)                               as tier,

        min(order_date)                         as first_order_date,
        max(order_date)                         as last_order_date,
        count(*)                                as orders_lifetime,
        count(*) filter (where order_date > {{ ds_minus(365) }})    as orders_12m,
        count(*) filter (where order_date > {{ ds_minus(90) }})     as orders_90d,
        count(*) filter (where order_date > {{ ds_minus(30) }})     as orders_30d,
        count(*) filter (where order_date > {{ ds_minus(7) }})      as orders_7d,

        sum(booked_cents)                       as booked_cents_lifetime,
        sum(net_sales_cents)                    as net_sales_cents_lifetime,
        sum(net_sales_cents) filter (where order_date > {{ ds_minus(365) }}) as net_sales_cents_12m,
        sum(returned_cents)                     as returned_cents_lifetime,
        sum(net_units)                          as net_units_lifetime,
        count(distinct channel)                 as channels_used

    from keyed
    group by 1, 2

)

select
    party_key,
    party_kind,
    party_kind = 'guest'                        as is_guest,
    coalesce(source_book, 'copperline')         as source_book,
    account_name,
    market_code,
    region_code,
    tier,

    -- §SD-3: a bucket has no life, so the two dates and the two durations are
    -- empty on it. Everything below them stays, because the counts and the
    -- money on a guest row are real.
    case when party_kind <> 'guest' then first_order_date end
                                                as first_order_date,
    case when party_kind <> 'guest' then last_order_date end
                                                as last_order_date,
    case when party_kind <> 'guest'
         then date_diff('day', first_order_date, {{ ds() }}) end
                                                as tenure_days,
    case when party_kind <> 'guest'
         then date_diff('day', last_order_date, {{ ds() }}) end
                                                as days_since_last_order,

    orders_lifetime,
    orders_12m,
    orders_90d,
    orders_30d,
    orders_7d,
    channels_used,

    booked_cents_lifetime,
    net_sales_cents_lifetime,
    coalesce(net_sales_cents_12m, 0)            as net_sales_cents_12m,
    returned_cents_lifetime,
    net_units_lifetime,

    case
        when orders_lifetime > 0
        then cast(round(cast(net_sales_cents_lifetime as decimal(38, 4)) / orders_lifetime, 0) as bigint)
    end                                         as average_order_net_cents,

    -- Stated, not inferred. 180 days, per docs/semantic-definitions.md §SD-3,
    -- and empty on a bucket because a bucket has no life to churn out of.
    case when party_kind <> 'guest'
         then date_diff('day', last_order_date, {{ ds() }}) > 180 end
                                                as is_churned,
    case when party_kind <> 'guest' then orders_lifetime = 1 end
                                                as is_one_and_done,
    case when party_kind <> 'guest'
         then first_order_date > {{ ds_minus(90) }} end
                                                as is_new

from lifecycle
