{{ config(materialized='view') }}

{#-
    Tickets, put on the right customer.

    `party_ref` has to be read against `party_source`. An `oms` ticket names a
    trade account; an `nwv` ticket names a Northwave account. The two id spaces
    overlap enough that reading `party_ref` on its own **succeeds** and puts the
    ticket on the wrong customer — which is worse than failing, because nothing
    complains.

    Not every `oms` ticket names an account that exists. The service desk takes
    tickets from consumers too, and a consumer has no trade account; those rows
    keep the reference and `party_resolves` is false.
-#}

with tickets as (

    select * from {{ ref('stg_support__tickets') }}

),

resolved as (

    select party_ref, ref_shape, customer_key, source_book, account_name, region_code, status
    from {{ ref('int_customer_resolved') }}

)

select
    t.ticket_id,
    t.party_ref,
    t.party_source,
    r.customer_key,
    r.source_book,
    r.account_name,
    r.region_code,
    r.status                                    as account_status,

    t.order_id,
    t.channel,
    t.category,
    t.category_area,
    t.csat,

    t.opened_at,
    t.closed_at,
    t.opened_date,
    t.closed_date,
    t.is_open,
    case
        when t.closed_at is not null
        then date_diff('hour', t.opened_at, t.closed_at)
    end                                         as resolution_hours,

    r.customer_key is not null                  as party_resolves,
    t.party_source = 'nwv'                      as is_northwave_book

from tickets t
left join resolved r
       on r.party_ref = t.party_ref
      and ((t.party_source = 'nwv' and r.ref_shape = 'northwave')
        or (t.party_source = 'oms' and r.ref_shape in ('current', 'legacy')))
