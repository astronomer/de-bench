{{ config(materialized='table') }}

{#-
    Output tax by day, entity and market.

    Tax-exclusive sales have no reserved name in docs/semantic-definitions.md
    and the tax team keeps its own columns; nobody has asked to conform them. So
    this table publishes `taxable_cents` and `tax_cents` under its own
    definitions and does not use a reserved name for either.
-#}

with order_tax as (

    select
        order_date                              as ds,
        market_code,
        channel,
        currency_code,
        sum(tax_cents)                          as order_tax_cents,
        sum(booked_cents)                       as order_taxable_cents,
        count(*)                                as order_count
    from {{ ref('int_orders_enriched') }}
    group by 1, 2, 3, 4

),

invoice_tax as (

    select
        invoice_date                            as ds,
        geography_key                           as market_code,
        entity_code,
        currency_key                            as currency_code,
        sum(line_tax_cents)                     as invoice_tax_cents,
        sum(line_net_cents)                     as invoice_taxable_cents,
        count(*)                                as invoice_count
    from {{ ref('fct_ar_invoices') }}
    group by 1, 2, 3, 4

),

days as (

    select ds, market_code, currency_code from order_tax
    union
    select ds, market_code, currency_code from invoice_tax

)

select
    d.ds                                        as date_key,
    d.ds,
    d.market_code                               as geography_key,
    d.market_code,
    d.currency_code                             as currency_key,
    d.currency_code,
    g.entity_code                               as entity,

    coalesce(sum(o.order_tax_cents), 0)         as order_tax_cents,
    coalesce(sum(o.order_taxable_cents), 0)     as order_taxable_cents,
    coalesce(sum(o.order_count), 0)             as order_count,

    coalesce(sum(i.invoice_tax_cents), 0)       as invoice_tax_cents,
    coalesce(sum(i.invoice_taxable_cents), 0)   as invoice_taxable_cents,
    coalesce(sum(i.invoice_count), 0)           as invoice_count,

    coalesce(sum(o.order_tax_cents), 0) + coalesce(sum(i.invoice_tax_cents), 0) as tax_cents,
    coalesce(sum(o.order_taxable_cents), 0) + coalesce(sum(i.invoice_taxable_cents), 0) as taxable_cents,

    cast(round(
        (cast(coalesce(sum(o.order_tax_cents), 0) as decimal(38, 4)) + coalesce(sum(i.invoice_tax_cents), 0))
        * 10000
        / nullif(coalesce(sum(o.order_taxable_cents), 0) + coalesce(sum(i.invoice_taxable_cents), 0), 0), 0
    ) as integer)                               as effective_tax_rate_bps

from days d
left join order_tax o on o.ds = d.ds and o.market_code = d.market_code and o.currency_code = d.currency_code
left join invoice_tax i on i.ds = d.ds and i.market_code = d.market_code and i.currency_code = d.currency_code
left join {{ ref('dim_geography') }} g on g.market_code = d.market_code
group by 1, 2, 3, 4, 5, 6, 7
