{{
    config(
        materialized='table'
    )
}}

{#-
    Cost centres. In this estate a cost centre is a merchandising department.

    There is no separate cost-centre master; the departments are what the
    category tree and the cost-complement table both key on, so they are the
    grain. `cost_complement_bps` is the current one — the FY2026 valuation
    change made the complement the way inventory is valued, at department
    grain, and docs/inventory-policy.md holds the method.

    `ALL` is a real department code in the category tree: it is the root, and it
    is excluded here because a root is not a cost centre.
-#}

with departments as (

    select
        dept_code,
        count(*)                        as category_count,
        count(*) filter (where is_current) as current_category_count,
        min(valid_from)                 as first_seen_on
    from {{ ref('stg_product__categories') }}
    where dept_code <> 'ALL'
    group by 1

),

complement as (

    select
        dept_code,
        fiscal_month,
        cost_complement_bps,
        effective_from,
        approved_by,
        row_number() over (
            partition by dept_code
            order by case when effective_from <= {{ ds() }} then 0 else 1 end,
                     effective_from desc
        ) as recency
    from {{ ref('stg_reference__dept_cost_complement') }}

),

held as (

    select
        dept_code,
        count(distinct sku)             as skus_held,
        count(distinct location_id)     as locations_holding,
        max(snapshot_date)              as last_snapshot_date
    from {{ ref('stg_inventory__snapshots') }}
    group by 1

),

unknown_member as (

    -- Most order lines carry a SKU the catalog does not hold, so their
    -- department is not knowable. They go here rather than into a NULL group: a
    -- NULL group silently disappears from every `group by` that touches it, and
    -- this cost centre is about a tenth of the trading.
    select
        'UNASSIGNED'                            as cost_center_key,
        'UNASSIGNED'                            as dept_code,
        0                                       as category_count,
        0                                       as current_category_count,
        cast(null as date)                      as first_seen_on,
        cast(null as integer)                   as current_cost_complement_bps,
        cast(null as varchar)                   as complement_fiscal_month,
        cast(null as date)                      as complement_effective_from,
        cast(null as varchar)                   as complement_approved_by,
        false                                   as has_cost_complement,
        0                                       as skus_held,
        0                                       as locations_holding,
        cast(null as date)                      as last_snapshot_date

),

departments_final as (

    select
        d.dept_code                                 as cost_center_key,
        d.dept_code,
        d.category_count,
        d.current_category_count,
        d.first_seen_on,

        c.cost_complement_bps                       as current_cost_complement_bps,
        c.fiscal_month                              as complement_fiscal_month,
        c.effective_from                            as complement_effective_from,
        c.approved_by                               as complement_approved_by,
        c.dept_code is not null                     as has_cost_complement,

        coalesce(h.skus_held, 0)                    as skus_held,
        coalesce(h.locations_holding, 0)            as locations_holding,
        h.last_snapshot_date

    from departments d
    left join complement c on c.dept_code = d.dept_code and c.recency = 1
    left join held h on h.dept_code = d.dept_code

)

select * from departments_final
union all
select * from unknown_member
