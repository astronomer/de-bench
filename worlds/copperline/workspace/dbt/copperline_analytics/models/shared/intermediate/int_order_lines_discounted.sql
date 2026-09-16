{{
    config(
        materialized='view'
    )
}}

{#-
    Allocate the header discount down to the line.

    An order carries two kinds of discount. Line promotions are already inside
    `line_total_cents` — the OMS applied them when it priced the line. Header
    promotions are a single number on the order and belong to no line.

    Every model that wants margin, net sales or a per-product number needs the
    header discount spread across the lines, and every model that spreads it
    itself spreads it slightly differently. So it is spread here, once.

    The method, and why it is this one:

    * Pro rata by tax-exclusive line total. A discount on the order reduces what
      the customer paid for each line in proportion to what the line cost.
    * Integer cents throughout, largest remainder. Divide the header discount by
      the line's share and you get a fraction of a cent. Rounding each line
      independently loses or invents cents, and the loss shows up in the
      reconciliation ties as a wandering one-cent gap. Largest remainder gives
      every line its floor and then hands the leftover cents, one each, to the
      lines with the biggest fraction. The allocation sums to the header
      discount exactly, on every order, always.
    * An order whose lines sum to zero gets no allocation. There is nothing to
      allocate against and dividing by it is how this model used to fail.
-#}

with lines as (

    select
        l.order_line_key,
        l.order_id,
        l.order_line_id,
        l.line_no,
        l.sku,
        l.qty,
        l.unit_price_cents,
        l.line_discount_cents,
        l.tax_cents,
        l.line_total_cents
    from {{ ref('stg_sales__order_lines') }} l

),

headers as (

    select
        order_id,
        order_date,
        channel,
        market_code,
        store_id,
        order_discount_cents,
        subtotal_cents
    from {{ ref('stg_sales__orders') }}

),

joined as (

    select
        l.*,
        h.order_date,
        h.channel,
        h.market_code,
        h.store_id,
        h.order_discount_cents,
        sum(l.line_total_cents) over (partition by l.order_id) as order_line_total_cents
    from lines l
    join headers h on h.order_id = l.order_id

),

floors as (

    select
        *,
        case
            when order_line_total_cents > 0
            then cast(
                (cast(order_discount_cents as decimal(38, 6)) * line_total_cents)
                / order_line_total_cents
                as decimal(38, 6)
            )
            else cast(0 as decimal(38, 6))
        end as exact_share
    from joined

),

remainders as (

    select
        *,
        cast(floor(exact_share) as bigint) as floor_cents,
        exact_share - floor(exact_share)   as fraction,
        order_discount_cents
            - sum(cast(floor(exact_share) as bigint)) over (partition by order_id)
                                           as cents_to_hand_out,
        row_number() over (
            partition by order_id
            order by exact_share - floor(exact_share) desc, line_total_cents desc, line_no
        )                                  as remainder_rank
    from floors

)

select
    order_line_key,
    order_id,
    order_line_id,
    line_no,
    order_date,
    channel,
    market_code,
    store_id,
    sku,
    qty,
    unit_price_cents,
    tax_cents,

    -- what the OMS already took off the line
    line_discount_cents,

    -- the line as the OMS priced it, before the header discount
    line_total_cents,

    -- this line's share of the order-level discount
    case
        when remainder_rank <= cents_to_hand_out then floor_cents + 1
        else floor_cents
    end                                             as header_discount_cents,

    -- what the line is worth once both discounts are off it
    line_total_cents
        - case
            when remainder_rank <= cents_to_hand_out then floor_cents + 1
            else floor_cents
          end                                       as discounted_line_cents,

    line_discount_cents
        + case
            when remainder_rank <= cents_to_hand_out then floor_cents + 1
            else floor_cents
          end                                       as total_discount_cents

from remainders
