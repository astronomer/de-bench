{{ config(materialized='table') }}

{#-
    Freight at the grain it is billed at: the package.

    A shipment-level freight number is a sum over this table and never a column
    on the shipment. Four packages on one shipment can cross four weight breaks
    and three zones, and the shipment carries none of that.

    There is no rate-card feed, so this table reports what the carrier billed
    and cannot say whether the bill was right. A task that wants the rate check
    authors the rate cards first.
-#}

select
    p.package_id                                as package_key,
    p.package_id,
    p.shipment_id                               as shipment_key,
    p.order_id                                  as order_key,

    p.ship_date                                 as date_key,
    p.ship_date,
    p.delivered_date,

    {{ dbt_utils.surrogate_key(['p.carrier_code', 'p.service_level']) }} as carrier_key,
    p.carrier_code,
    p.service_level,
    p.lane_id,
    p.origin_dc                                 as warehouse_key,
    p.dest_zip3,
    p.dest_country,
    p.brand,
    p.zone,

    p.billed_weight_g,
    p.billed_cents,
    p.accessorial_cents,
    p.total_freight_cents,

    cast(round(
        cast(p.total_freight_cents as decimal(38, 4)) * 1000 / nullif(p.billed_weight_g, 0), 0
    ) as bigint)                                as cost_per_kg_cents,

    p.accessorial_cents > 0                     as has_accessorial

from {{ ref('stg_logistics__shipment_packages') }} p
