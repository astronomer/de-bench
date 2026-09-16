{{ config(materialized='table') }}

{#-
    The product dimension.

    **This is a conformed dimension and commerce builds it.** The stated policy
    is that the platform team builds conformed dims once and no team forks one —
    projects/platform/README.md, rule 5. This one never finished moving. It is
    built here, from commerce's snapshot pair, under a platform contract it has
    to satisfy, and it has been on the "to move" list for four quarters.

    Two sources, and they are two because a price change and an attribute change
    have different audiences. `snap_product_price` carries list price, which
    pricing reads daily. `snap_product_attrs` carries name, category, brand,
    supplier and status, which merchandising reads when something looks wrong.

    A SKU appears here whether or not it has ever sold. `is_catalogued` on the
    facts says the other direction: **the order book uses a wider SKU range than
    the catalog carries**, so most order lines have no product row. That is a
    real seam in this estate, not a modelling choice, and every fact that joins
    this dimension has to keep the unmatched lines rather than dropping them.
-#}

with price as (

    select
        sku,
        list_price_cents,
        updated_at              as price_updated_at,
        dbt_valid_from          as price_valid_from,
        dbt_valid_to            as price_valid_to
    from {{ ref('snap_product_price') }}
    where dbt_valid_to is null

),

attrs as (

    select
        sku,
        product_name,
        category_id,
        brand,
        supplier_id,
        status,
        attr_color,
        attr_size,
        updated_at              as attrs_updated_at,
        dbt_valid_from          as attrs_valid_from
    from {{ ref('snap_product_attrs') }}
    where dbt_valid_to is null

),

categories as (

    select category_id, category_name, dept_code, parent_id
    from {{ ref('stg_product__categories') }}
    where is_current

),

history as (

    select
        sku,
        count(*)                                        as change_count,
        min(updated_at)                                 as first_seen_at,
        max(updated_at)                                 as last_changed_at,
        count(*) filter (where operation = 'delete')    as delete_count
    from {{ ref('stg_product__pim_versions') }}
    group by 1

)

select
    a.sku                                       as product_key,
    a.sku,
    a.product_name,
    a.brand,
    a.supplier_id,
    a.status,
    a.status = 'active'                         as is_active,
    a.attr_color,
    a.attr_size,

    a.category_id,
    c.category_name,
    c.dept_code,
    c.parent_id                                 as parent_category_id,

    p.list_price_cents,
    p.price_updated_at,
    p.price_valid_from,
    a.attrs_updated_at,

    h.change_count,
    h.first_seen_at,
    h.last_changed_at,
    coalesce(h.delete_count, 0) > 0             as was_deleted_upstream

from attrs a
left join price p on p.sku = a.sku
left join categories c on c.category_id = a.category_id
left join history h on h.sku = a.sku
