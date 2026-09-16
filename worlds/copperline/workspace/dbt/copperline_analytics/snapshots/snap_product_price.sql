{% snapshot snap_product_price %}
{{
    config(
        target_schema='snapshots',
        unique_key='sku',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

{#-
    List price history, one row per SKU per price.

    **Reads the source, not the staging view.** A snapshot that reads a model
    cannot run until that model has been built, which makes the nightly's order
    `run`, `snapshot`, `run` again — and dim_product sits on the far side of
    that. Against the source it is `snapshot` then `run`, from cold, every time.
    The cost is that the PIM's source tie-break is spelled here as well as in
    `stg_product__pim_versions`; if that rule changes, it changes in two places.

    Split from the attribute snapshot on purpose. Pricing changes far more often
    than a product's name or its category does, and one snapshot over both would
    mint a version row every time either moved — which is how the FY2024 version
    of this table reached nine versions per SKU per quarter and nobody could
    find the price change in it.
-#}

with ranked as (

    select
        sku,
        list_price_cents,
        updated_at,
        row_number() over (
            partition by sku
            order by
                updated_at desc,
                case source
                    when 'pim_ui' then 1
                    when 'supplier_feed' then 2
                    when 'bulk_load' then 3
                    else 4
                end,
                change_id
        ) as version_seq
    from {{ source('product', 'pim_product_versions') }}
    where operation = 'upsert'

)

select
    sku,
    cast(list_price_cents as bigint)    as list_price_cents,
    updated_at
from ranked
where version_seq = 1

{% endsnapshot %}
