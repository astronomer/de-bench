{{ config(materialized='table') }}

{#-
    Everything that should tie and does not, in one table, one row per exception.

    The seams this warehouse actually has, each one a `check_name`:

    * `pos_variance`         the till and the order book disagree on a store-day.
    * `shadow_quarter`       a payment in both processor books.
    * `ambiguous_payment`    a recycled order reference matched more than one order.
    * `gift_card_applied`    the order header and the gift-card ledger disagree.
    * `unsettled_marketplace` a marketplace order with no settlement lines.
    * `commission_variance`  settlement took a different rate than the order stated.

    It is one table rather than six so that "what is broken this morning" is one
    query. The threshold on each check is stated in the row, not hidden in a
    where clause, so a reader can see what was tolerated.
-#}

with pos_variance as (

    select
        ds,
        'pos_variance'                          as check_name,
        store_id                                as subject,
        'store-day'                             as subject_kind,
        abs(variance_cents)                     as variance_cents,
        'till net against order book net'       as description
    from {{ ref('agg_daily_store_sales') }}
    where abs(variance_cents) > 0

),

shadow_quarter as (

    select
        order_date                              as ds,
        'shadow_quarter'                        as check_name,
        order_id                                as subject,
        'order'                                 as subject_kind,
        abs(meridian_captured_cents + halcyon_settled_cents - grand_total_cents) as variance_cents,
        'settled by both processors'            as description
    from {{ ref('int_payment_matched') }}
    where is_shadow_quarter

),

ambiguous_payment as (

    select
        order_date                              as ds,
        'ambiguous_payment'                     as check_name,
        order_ref                               as subject,
        'order reference'                       as subject_kind,
        cast(0 as bigint)                       as variance_cents,
        'recycled reference matched more than one order' as description
    from {{ ref('int_payment_matched') }}
    where halcyon_match_is_ambiguous or payment_match_is_ambiguous

),

gift_card_applied as (

    select
        o.order_date                            as ds,
        'gift_card_applied'                     as check_name,
        o.order_id                              as subject,
        'order'                                 as subject_kind,
        abs(o.gift_card_applied_cents - coalesce(g.redeemed_cents, 0)) as variance_cents,
        'order header against gift-card ledger' as description
    from {{ ref('fct_order') }} o
    left join (
        select order_id, sum(amount_cents) as redeemed_cents
        from {{ ref('stg_sales__gift_card_ledger') }}
        where entry_type = 'redeem' and order_id is not null
        group by 1
    ) g on g.order_id = o.order_id
    where o.gift_card_applied_cents <> coalesce(g.redeemed_cents, 0)

),

unsettled_marketplace as (

    select
        placed_date                             as ds,
        'unsettled_marketplace'                 as check_name,
        marketplace_order_id                    as subject,
        'marketplace order'                     as subject_kind,
        gmv_cents                               as variance_cents,
        'no settlement lines'                   as description
    from {{ ref('int_settlement_matched') }}
    where not is_settled

),

commission_variance as (

    select
        placed_date                             as ds,
        'commission_variance'                   as check_name,
        marketplace_order_id                    as subject,
        'marketplace order'                     as subject_kind,
        abs(commission_rate_variance_cents)     as variance_cents,
        'settled commission against the header rate' as description
    from {{ ref('int_settlement_matched') }}
    where abs(commission_rate_variance_cents) > 100

),

all_exceptions as (

    select * from pos_variance
    union all select * from shadow_quarter
    union all select * from ambiguous_payment
    union all select * from gift_card_applied
    union all select * from unsettled_marketplace
    union all select * from commission_variance

)

select
    {{ dbt_utils.surrogate_key(['check_name', 'subject', 'ds']) }} as exception_key,
    ds                                          as date_key,
    ds,
    check_name,
    subject_kind,
    subject,
    variance_cents,
    description,
    ds >= {{ ds_minus(7) }}                     as is_recent
from all_exceptions
