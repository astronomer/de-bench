{{ config(materialized='view') }}

{#-
    Returns, linked to the line they came off and the money that went back.

    The return feed cites the order and the line ordinal. The refund method says
    where the money went, and that matters more than it looks: an `original`
    refund reverses a card payment and shows up in the processor books, a
    `gift_card` refund creates a liability and shows up in the gift-card ledger,
    and a `store_credit` refund shows up in neither. A returns number that adds
    all three and ties it to the card settlements will never tie.

    `days_to_return` is measured from the order to the **initiation**, because
    that is the customer's decision. `days_in_transit` is initiation to receipt
    and belongs to the carrier.
-#}

with returns as (

    select * from {{ ref('stg_sales__returns') }}

),

lines as (

    select
        order_id,
        order_line_id,
        order_line_key,
        line_no,
        sku,
        order_date,
        channel,
        market_code,
        store_id,
        qty                     as ordered_qty,
        discounted_line_cents,
        net_sales_cents
    from {{ ref('int_net_sales_lines') }}

)

select
    r.rma_id,
    r.order_id,
    r.order_line_id,
    l.order_line_key,
    l.line_no,
    coalesce(l.sku, r.sku)                          as sku,
    l.order_date,
    l.channel,
    l.market_code,
    l.store_id,

    r.return_reason,
    r.return_channel,
    r.disposition,
    r.refund_method,
    r.restock_flag,

    r.qty                                           as returned_qty,
    l.ordered_qty,
    r.refund_cents,
    l.discounted_line_cents                         as line_value_cents,

    r.initiated_date,
    r.received_date,
    date_diff('day', l.order_date, r.initiated_date) as days_to_return,
    date_diff('day', r.initiated_date, r.received_date) as days_in_transit,

    -- Where the money went. Each one ties to a different book, or to none.
    r.refund_method = 'original'                    as refunded_to_card,
    r.refund_method = 'gift_card'                   as refunded_to_gift_card,
    r.refund_method = 'store_credit'                as refunded_to_store_credit,

    r.received_date > {{ ds() }}                    as is_in_flight,
    l.order_line_key is null                        as line_not_found,
    r.disposition = 'restock'                       as returns_to_stock

from returns r
left join lines l on l.order_id = r.order_id and l.order_line_id = r.order_line_id
