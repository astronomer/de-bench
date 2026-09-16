{{ config(materialized='table') }}

{#-
    Tickets, resolution and satisfaction by day and area.

    The service level is stated in the model, not read from a feed: payment
    tickets in twenty-four hours, everything else in forty-eight. There is no
    service-level table upstream, so a change to it is a change to this file and
    it moves every number here.

    An open ticket is excluded from the SLA rate rather than counted as a
    breach. A ticket opened an hour ago has not missed anything.
-#}

select
    t.opened_date                               as date_key,
    t.opened_date                               as ds,
    t.category_area,
    t.channel,
    t.source_book,

    count(*)                                    as tickets_opened,
    count(*) filter (where t.is_open)           as tickets_still_open,
    count(*) filter (where not t.is_open)       as tickets_closed,
    count(*) filter (where t.met_sla)           as tickets_within_sla,
    count(*) filter (where t.met_sla = false)   as tickets_breached_sla,
    count(*) filter (where not t.party_resolves) as tickets_without_account,

    cast(round(avg(t.resolution_hours), 1) as decimal(8, 1)) as average_resolution_hours,
    max(t.resolution_hours)                     as worst_resolution_hours,
    cast(round(avg(t.csat), 2) as decimal(5, 2)) as average_csat,
    count(t.csat)                               as csat_responses,

    cast(round(
        cast(count(*) filter (where t.met_sla) as decimal(38, 4)) * 10000
        / nullif(count(*) filter (where not t.is_open), 0), 0
    ) as integer)                               as sla_rate_bps

from {{ ref('fct_tickets') }} t
group by 1, 2, 3, 4, 5
