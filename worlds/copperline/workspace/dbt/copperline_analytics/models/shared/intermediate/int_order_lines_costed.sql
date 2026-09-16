{{
    config(
        materialized='view'
    )
}}

{#-
    Landed unit cost at the order date.

    Cost of goods is commerce's margin number and finance's cost of sales. They
    have to be the same number, so it is computed here and neither team computes
    it again. That is rule 4, and this model is the reason the rule exists: for
    most of FY2024 the merch dashboard and the P&L disagreed on gross margin by
    about two points, and both were defensible.

    Where the cost comes from, in order:

    1. `unit_cost_cents` on the order line. The OMS stamps the standard cost at
       the moment the line is priced, so it is the cost as of the order date and
       needs no lookup.
    2. Nothing else. There is no second source. A line with no stated cost is
       marked `cost_is_stated = false` and carries a NULL cost, and every model
       downstream of this one has to decide what to do about it rather than
       silently treating it as zero. `marts.category_margin` excludes those
       lines from the margin rate and reports how many it excluded.

    A NULL cost is not rare and it is not a defect. From the FY2026 valuation
    change the inventory feed stopped carrying a unit cost at all, and the
    method moved to a cost complement at department grain —
    docs/inventory-policy.md. This model does not apply that method; it reports
    what the order line stated. `fct_inventory_valuation` is where the
    complement is applied.
-#}

with lines as (

    select
        order_line_key,
        order_id,
        line_no,
        sku,
        qty,
        unit_cost_cents,
        line_total_cents
    from {{ ref('stg_sales__order_lines') }}

),

orders as (

    select order_id, order_date, channel
    from {{ ref('stg_sales__orders') }}

)

select
    l.order_line_key,
    l.order_id,
    l.line_no,
    o.order_date,
    o.channel,
    l.sku,
    l.qty,

    l.unit_cost_cents,
    l.unit_cost_cents is not null                as cost_is_stated,

    -- qty is a decimal upstream because a trade line can be a part quantity.
    -- The extension rounds half-up to the cent, once, and stays an integer.
    case
        when l.unit_cost_cents is not null
        then cast(round(cast(l.qty as decimal(18, 4)) * l.unit_cost_cents, 0) as bigint)
    end                                          as extended_cost_cents,

    l.line_total_cents

from lines l
join orders o on o.order_id = l.order_id
