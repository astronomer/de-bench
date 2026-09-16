{{
    config(
        materialized='view'
    )
}}

{#- The PIM change feed.

    Three sources write it and they disagree about the same SKU on the same day.
    The tie-break is pim_ui, then supplier_feed, then bulk_load, and it is
    applied here so that every reader gets the same version of a change. A
    `delete` operation is kept: it is how the PIM retires a SKU and dim_product
    needs to see it.
-#}

with ranked as (

    select
        *,
        case source
            when 'pim_ui' then 1
            when 'supplier_feed' then 2
            when 'bulk_load' then 3
            else 4
        end as source_rank
    from {{ source('product', 'pim_product_versions') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by sku, cast(updated_at as date)
            order by source_rank, updated_at desc, change_id
        ) as version_seq
    from ranked

)

select
    change_id,
    sku,
    product_name,
    category_id,
    brand,
    supplier_id,
    cast(list_price_cents as bigint)    as list_price_cents,
    status,
    operation,
    source,
    source_rank,
    attributes_json,
    json_extract_string(attributes_json, '$.color')  as attr_color,
    json_extract_string(attributes_json, '$.size')   as attr_size,
    updated_at,
    received_at,
    cast(updated_at as date)            as updated_date
from deduped
where version_seq = 1
