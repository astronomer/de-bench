{{
    config(
        materialized='table'
    )
}}

{#-
    Carriers and the service levels they run.

    The grain is the pair, not the carrier. A carrier's next-day service and its
    freight service are different products with different lanes, different
    weight breaks and different people to shout at, and every model that has
    ever scored "the carrier" has ended up re-splitting them.
-#}

with packages as (

    select
        carrier_code,
        service_level,
        count(*)                                    as package_count,
        count(distinct shipment_id)                 as shipment_count,
        count(distinct lane_id)                     as lane_count,
        min(ship_date)                              as first_ship_date,
        max(ship_date)                              as last_ship_date,
        sum(total_freight_cents)                    as billed_cents,
        cast(round(avg(billed_weight_g), 0) as bigint) as average_weight_g,
        min(zone)                                   as min_zone,
        max(zone)                                   as max_zone
    from {{ ref('stg_logistics__shipment_packages') }}
    group by 1, 2

),

invoiced as (

    select
        carrier_code,
        count(*)            as invoice_count,
        sum(total_cents)    as invoiced_cents,
        max(period_end)     as last_invoiced_period_end
    from {{ ref('stg_logistics__carrier_invoices') }}
    group by 1

)

select
    {{ dbt_utils.surrogate_key(['p.carrier_code', 'p.service_level']) }} as carrier_key,
    p.carrier_code,
    p.service_level,
    p.carrier_code || ' ' || p.service_level        as carrier_service_name,

    case
        when p.service_level like 'express%' then 'express'
        when p.service_level like 'freight%' then 'freight'
        else 'ground'
    end                                             as service_family,

    p.package_count,
    p.shipment_count,
    p.lane_count,
    p.first_ship_date,
    p.last_ship_date,
    p.billed_cents,
    p.average_weight_g,
    p.min_zone,
    p.max_zone,

    coalesce(i.invoice_count, 0)                    as carrier_invoice_count,
    coalesce(i.invoiced_cents, 0)                   as carrier_invoiced_cents,
    i.last_invoiced_period_end,

    p.last_ship_date > {{ ds_minus(30) }}           as is_active

from packages p
left join invoiced i on i.carrier_code = p.carrier_code
