{{
    config(
        materialized='table'
    )
}}

{#-
    The store estate, one row per store.

    The source is SCD2 and this dimension is the current row. A model that wants
    the estate as it was on a date reads stg_store__stores and picks its own
    version; this is deliberately not a type-2 dimension, because every consumer
    that has ever asked for one wanted the current store name on a historic
    order, and giving them a version row instead produced two rows per order.

    `first_comp_date` is the load-bearing column. A store enters comparable
    sales thirteen full fiscal months after it opens, and an **acquired** store
    thirteen full fiscal months after the acquisition close, not after its own
    opening date. Forty-four Northwave stores opened between 2011 and 2023 and
    applying the opening-date rule to them puts every one of them into comp
    about a year early. docs/comp-store-policy.md holds both rules; this column
    is where they are applied, once, so that no mart has to remember which store
    came from where.
-#}

with stores as (

    select *
    from {{ ref('stg_store__stores') }}
    where is_current

),

versions as (

    select
        store_id,
        min(valid_from)     as first_seen_on,
        count(*)            as version_count
    from {{ ref('stg_store__stores') }}
    group by 1

)

select
    s.store_id                                  as store_key,
    s.store_id,
    s.store_name,
    s.store_format,
    s.region_code,
    s.market_code,
    s.tz_name,
    s.status,
    s.status = 'open'                           as is_open,
    s.acquired_from,
    s.source_book,
    s.acquired_from is not null                 as is_acquired,

    s.opened_on,
    s.closed_on,
    v.first_seen_on,
    v.version_count,

    -- Thirteen full fiscal months. For an acquired store the clock starts at
    -- the acquisition close, not at the store's own opening.
    case
        when s.acquired_from = 'northwave'
        then (date '2025-02-03' + interval '13 months')::date
        else (s.opened_on + interval '13 months')::date
    end                                         as first_comp_date,

    case
        when s.acquired_from = 'northwave' then 'acquisition close'
        else 'store opening'
    end                                         as comp_clock_starts_at,

    date_diff('day', s.opened_on, {{ ds() }})   as days_open

from stores s
left join versions v on v.store_id = s.store_id
