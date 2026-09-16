{{
    config(
        materialized='table'
    )
}}

{#-
    Suppliers, as far as the estate knows them.

    There is no supplier master feed. The only place a supplier appears is on a
    PIM product version, so this dimension is what can be derived from the
    assortment: which supplier a SKU belongs to, how many SKUs they hold, which
    departments they sell into, and when their products last changed.

    **There is no spend here and there cannot be.** Purchase orders and goods
    receipts are not extracted, so supplier spend, on-time-in-full and the
    scorecard have nothing to build from. A task that wants them authors the
    feed first. Until then this dimension is an assortment view and every model
    that joins it says so.
-#}

with versions as (

    select *
    from {{ ref('stg_product__pim_versions') }}
    where supplier_id is not null

),

latest_per_sku as (

    select
        sku,
        supplier_id,
        status,
        category_id,
        list_price_cents,
        updated_at,
        row_number() over (partition by sku order by updated_at desc, change_id desc) as version_seq
    from versions

),

current_skus as (

    select l.*, c.dept_code
    from latest_per_sku l
    left join {{ ref('stg_product__categories') }} c on c.category_id = l.category_id
    where l.version_seq = 1

)

select
    supplier_id                                     as supplier_key,
    supplier_id,
    count(*)                                        as sku_count,
    count(*) filter (where status = 'active')       as active_sku_count,
    count(*) filter (where status = 'discontinued') as discontinued_sku_count,
    count(distinct dept_code)                       as department_count,
    string_agg(distinct dept_code, ',' order by dept_code) as departments,
    min(list_price_cents)                           as min_list_price_cents,
    max(list_price_cents)                           as max_list_price_cents,
    cast(round(avg(list_price_cents), 0) as bigint) as average_list_price_cents,
    max(updated_at)                                 as last_change_at,
    count(*) filter (where status = 'active') > 0   as is_active

from current_skus
group by 1
