{{ config(materialized='view') }}

{#-
    The two SKU spellings, matched as far as they can be.

    The estate spells a SKU two ways and nothing upstream reconciles them:

    * the **catalog key**, `SKU-#####`, on order lines, invoice lines, returns
      and the clickstream;
    * the **merchandising code**, `BLD-1024-CLA-4PK-19024`, on inventory
      snapshots and warehouse movements.

    The merchandising code ends in a five-digit ordinal, and where that ordinal
    matches a catalog key the two are the same product. Where it does not — and
    for most of the estate it does not — there is no match and this model says
    so rather than guessing.

    **Read `match_method` before using a joined row.** `ordinal` is a real
    match. `none` means the inventory row and the sales row cannot be tied
    together, and any number that needs both is unavailable for that SKU. Supply
    keeps this local rather than pushing it to `int` because no other team joins
    the two spellings; the day one does, this moves.

    Fixing the spellings upstream would make this model unnecessary, which is
    the right outcome and is not this team's to make.
-#}

with merch_codes as (

    select distinct
        sku                                     as merch_sku,
        dept_code
    from {{ ref('stg_inventory__snapshots') }}

    union

    select distinct
        sku                                     as merch_sku,
        cast(null as varchar)                   as dept_code
    from {{ ref('stg_inventory__wms_movements') }}

),

deduped as (

    select
        merch_sku,
        max(dept_code)                          as dept_code
    from merch_codes
    group by 1

),

candidates as (

    select
        d.merch_sku,
        d.dept_code,
        case
            when right(d.merch_sku, 5) similar to '[0-9]{5}'
            then 'SKU-' || right(d.merch_sku, 5)
        end                                     as candidate_catalog_sku
    from deduped d

)

select
    c.merch_sku,
    c.dept_code,
    p.sku                                       as catalog_sku,
    p.product_key,
    p.category_id,
    p.dept_code                                 as catalog_dept_code,
    p.list_price_cents,
    case when p.sku is not null then 'ordinal' else 'none' end as match_method,
    p.sku is not null                           as is_matched
from candidates c
left join {{ ref('dim_product') }} p on p.sku = c.candidate_catalog_sku
