{{
    config(
        materialized='view'
    )
}}

{#-
    The order header, with its lines rolled up, its currency settled and its
    payment attached.

    Three jobs, all of them things every team was doing separately:

    1. **Currency.** `currency_code` is NULL on about half the orders and means
       the market's own currency — the OMS only writes the column when the
       customer paid in something other than the market default. The default per
       market is stated once, here. Every fact that converts carries
       `fx_rate_ppm` and `fx_rate_date` beside the converted amount so a reader
       can redo the arithmetic without the rate table.

    2. **Lines.** Line count, units and net sales, from int_net_sales_lines.
       `booked_cents` is the order value on the order date, gross, no
       recognition — a reserved name, and it never moves again, including for a
       refund. That is why it is the header's own subtotal and not a sum over
       the lines net of returns.

    3. **Payment.** The link from an order to its payment attempts runs through
       `order_ref`, and `order_ref` is recycled: the OMS reuses a seven-digit
       reference about every fifty days, so one reference names three orders
       over the life of the estate. Joining on it alone attaches the wrong
       payment to about one order in three. The attempt is matched to the
       nearest order within a seven-day window on either side, and the match is
       marked ambiguous when more than one order shares the reference in that
       window. Downstream models are expected to look at `payment_match_is_
       ambiguous` before they trust `payment_outcome`.
-#}

with orders as (

    select *
    from {{ ref('stg_sales__orders') }}

),

market_currency as (

    -- The market's own currency. Stated once. A market that starts trading in a
    -- new currency is a row here and a conversation, not a coalesce in a mart.
    select * from (
        values
            ('US', 'USD'),
            ('CA', 'CAD'),
            ('GB', 'GBP'),
            ('IE', 'EUR'),
            ('DE', 'EUR'),
            ('MX', 'MXN'),
            ('BR', 'BRL'),
            ('PL', 'PLN'),
            ('ID', 'IDR')
    ) as t (market_code, default_currency_code)

),

lines_rolled as (

    select
        order_id,
        count(*)                        as line_count,
        sum(qty)                        as ordered_units,
        sum(net_qty)                    as net_units,
        sum(gross_line_cents)           as gross_line_cents,
        sum(total_discount_cents)       as line_discount_total_cents,
        sum(returned_cents)             as returned_cents,
        sum(net_sales_cents)            as net_sales_cents,
        max(case when has_return then 1 else 0 end) = 1 as has_return
    from {{ ref('int_net_sales_lines') }}
    group by 1

),

resolved as (

    select
        o.*,
        coalesce(o.currency_code, m.default_currency_code) as settled_currency_code,
        o.currency_code is null                            as currency_was_defaulted
    from orders o
    left join market_currency m on m.market_code = o.market_code

),

with_rate as (

    select
        r.*,
        coalesce(r.fx_rate_ppm, f.fx_rate_ppm) as effective_fx_rate_ppm,
        case when r.fx_rate_ppm is not null then r.order_date else f.rate_date end as fx_rate_date
    from resolved r
    left join {{ ref('stg_reference__fx_rates') }} f
           on f.currency_code = r.settled_currency_code
          and f.rate_date = r.order_date

),

-- Payment attempts, matched through the recycled reference.
intents as (

    select
        intent_id,
        order_ref,
        attempt_no,
        outcome,
        created_at,
        created_date
    from {{ ref('stg_payments__payment_intents') }}

),

candidates as (

    select
        o.order_id,
        i.intent_id,
        i.outcome,
        i.attempt_no,
        i.created_at,
        abs(date_diff('day', o.order_date, i.created_date)) as day_gap,
        count(*) over (partition by o.order_id)             as candidate_count
    from with_rate o
    join intents i
      on i.order_ref = o.order_ref
     and i.created_date between o.order_date - 7 and o.order_date + 7

),

best_candidate as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by order_id
                order by day_gap, case outcome when 'succeeded' then 0 else 1 end, attempt_no
            ) as pick
        from candidates
    ) ranked
    where pick = 1

)

select
    o.order_id,
    o.order_ref,
    o.order_date,
    o.channel,
    o.brand,
    o.market_code,
    o.store_id,
    o.customer_ref,
    o.loyalty_id,
    o.source_system,
    o.order_status,
    o.event_time_utc,
    o.event_time_local,

    o.settled_currency_code                     as currency_code,
    o.currency_was_defaulted,
    o.effective_fx_rate_ppm                     as fx_rate_ppm,
    o.fx_rate_date,

    -- booked_cents: the order on the order date, gross, no recognition. It
    -- never moves again. docs/semantic-definitions.md.
    o.subtotal_cents                            as booked_cents,
    {{ to_base_cents('o.subtotal_cents', 'o.effective_fx_rate_ppm') }} as booked_base_cents,

    o.order_discount_cents,
    o.tax_cents,
    o.shipping_cents,
    o.grand_total_cents,
    o.gift_card_applied_cents,

    coalesce(l.line_count, 0)                   as line_count,
    coalesce(l.ordered_units, 0)                as ordered_units,
    coalesce(l.net_units, 0)                    as net_units,
    coalesce(l.gross_line_cents, 0)             as gross_line_cents,
    coalesce(l.line_discount_total_cents, 0)    as discount_cents,
    coalesce(l.returned_cents, 0)               as returned_cents,
    coalesce(l.net_sales_cents, 0)              as net_sales_cents,
    coalesce(l.has_return, false)               as has_return,

    p.intent_id                                 as payment_intent_id,
    p.outcome                                   as payment_outcome,
    p.attempt_no                                as payment_attempt_no,
    p.created_at                                as payment_attempted_at,
    coalesce(p.candidate_count, 0) > 1          as payment_match_is_ambiguous,
    p.intent_id is not null                     as has_payment_attempt,
    coalesce(p.outcome = 'succeeded', false)    as is_paid,

    o.updated_at,
    o.loaded_at

from with_rate o
left join lines_rolled l on l.order_id = o.order_id
left join best_candidate p on p.order_id = o.order_id
