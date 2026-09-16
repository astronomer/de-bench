{{ config(materialized='table') }}

{#-
    One row per service-desk ticket.

    Support goes straight from staging to the mart with no intermediate step of
    its own beyond the party link. There is no shared logic here and no grain to
    collapse, so no `int` model is earned. That thin case is deliberate: the
    middle layer is earned, not mandatory.
-#}

select
    t.ticket_id                                 as ticket_key,
    t.ticket_id,
    t.opened_date                               as date_key,
    t.opened_date,
    t.closed_date,
    t.opened_at,
    t.closed_at,

    t.customer_key,
    t.party_ref,
    t.party_source,
    t.account_name,
    t.region_code,
    t.source_book,
    t.order_id                                  as order_key,
    t.order_id,

    t.channel,
    t.category,
    t.category_area,
    t.csat,

    t.resolution_hours,
    t.is_open,
    t.party_resolves,
    t.is_northwave_book,

    case
        when t.resolution_hours is null then null
        when t.category_area = 'Payment' then t.resolution_hours <= 24
        else t.resolution_hours <= 48
    end                                         as met_sla

from {{ ref('int_customer_ticket_linked') }} t
