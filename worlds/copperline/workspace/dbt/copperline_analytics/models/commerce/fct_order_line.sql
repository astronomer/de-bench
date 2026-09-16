{{ config(materialized='incremental', unique_key='order_line_key', incremental_strategy='delete+insert') }}

{#-
    One row per order line. The widest published table in the warehouse.

    Incremental with a unique key, because it is over a million rows and
    projects/platform/README.md forbids a full refresh on the shared warehouse:
    the DuckDB file takes one writer and a full rebuild locks it for minutes
    while every other team's DAG waits.

    The incremental window is the trailing fourteen days of order date, not of
    load time. An order can be restated after it lands — a return, a status
    change, a settlement — and a load-time window misses every one of those.

    `is_catalogued` is false on most rows. The order book carries a wider SKU
    range than the product catalog does, so the product join misses; the line is
    kept and the flag says so. Dropping the unmatched lines loses about nine
    tenths of the revenue.
-#}

with lines as (

    select * from {{ ref('int_net_sales_lines') }}

    {% if is_incremental() %}
    where order_date >= {{ ds_minus(14) }}
    {% endif %}

),

costed as (

    select order_line_key, unit_cost_cents, extended_cost_cents, cost_is_stated
    from {{ ref('int_order_lines_costed') }}

),

products as (

    select product_key, sku, category_id, dept_code, brand, list_price_cents, supplier_id
    from {{ ref('dim_product') }}

)

select
    l.order_line_key,
    l.order_id,
    l.order_line_id,
    l.line_no,

    l.order_date                                as date_key,
    l.order_date,
    l.channel                                   as channel_key,
    l.channel,
    l.market_code                               as geography_key,
    l.store_id                                  as store_key,
    l.sku,
    p.product_key,
    p.category_id,
    p.dept_code                                 as cost_center_key,
    p.supplier_id                               as supplier_key,
    p.brand                                     as product_brand,
    l.brand                                     as order_brand,

    l.customer_ref,
    l.loyalty_id,
    l.order_status,
    l.source_system,

    l.qty,
    l.returned_qty,
    l.net_qty,

    l.gross_line_cents,
    l.line_discount_cents,
    l.header_discount_cents,
    l.total_discount_cents,
    l.discounted_line_cents,
    l.returned_cents,
    l.net_sales_cents,
    l.tax_cents,

    c.unit_cost_cents,
    c.extended_cost_cents,
    c.cost_is_stated,

    -- merch_margin_cents: net sales less landed cost. A reserved name;
    -- docs/semantic-definitions.md. NULL when the cost is not stated, never
    -- zero — a line with no cost has an unknown margin, not a full one.
    case
        when c.cost_is_stated
        then l.net_sales_cents - c.extended_cost_cents
    end                                         as merch_margin_cents,

    p.list_price_cents,
    p.product_key is not null                   as is_catalogued,
    l.has_return,
    l.return_count

from lines l
left join costed c on c.order_line_key = l.order_line_key
left join products p on p.sku = l.sku
