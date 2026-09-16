{{ config(materialized='view') }}

{#-
    A lane and a day: what moved, how long it took, what it cost.

    The grain a lane is managed at. It is here rather than in a mart because
    three of supply's marts need it — lane performance, the freight accrual and
    DC capacity — and each of them was rolling packages up its own way.
-#}

select
    s.ship_date                                 as ds,
    s.lane_id,
    s.origin_dc,
    s.dest_region,
    s.dest_country,
    s.carrier_code,
    s.service_family,

    count(*)                                            as shipment_count,
    sum(s.package_count)                                as package_count,
    sum(s.total_weight_g)                               as total_weight_g,
    sum(s.billed_weight_g)                              as billed_weight_g,
    sum(s.total_freight_cents)                          as freight_cents,
    sum(s.accessorial_cents)                            as accessorial_cents,

    count(*) filter (where s.is_delivered)              as delivered_count,
    count(*) filter (where s.is_open)                   as open_count,
    count(*) filter (where s.is_on_time)                as on_time_count,
    cast(round(avg(s.transit_hours), 1) as decimal(8, 1)) as average_transit_hours,
    max(s.transit_hours)                                as worst_transit_hours,
    max(s.expected_transit_hours)                       as expected_transit_hours,

    cast(round(
        cast(sum(s.total_freight_cents) as decimal(38, 4)) / nullif(sum(s.package_count), 0), 0
    ) as bigint)                                        as cost_per_package_cents

from {{ ref('int_scan_normalized') }} s
group by 1, 2, 3, 4, 5, 6, 7
