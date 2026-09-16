{{ config(materialized='table') }}

{#-
    Card and invoice disputes by day.

    It sits in commerce and not in finance because a dispute is a payment
    problem until it is settled, and the settlement it is argued against is
    commerce's. Finance reads the settled number, not the open one.
-#}

with invoice_disputes as (

    select
        raised_date                             as ds,
        dispute_status,
        reason_code,
        count(*)                                as dispute_count,
        sum(disputed_cents)                     as disputed_cents,
        sum(settled_cents)                      as settled_cents,
        count(*) filter (where is_open)         as open_count,
        cast(round(avg(days_to_resolve), 0) as integer) as average_days_to_resolve
    from {{ ref('stg_finance__disputes') }}
    group by 1, 2, 3

),

chargebacks as (

    select
        coalesce(settlement_date, cast(event_time_utc as date)) as ds,
        count(*)                                as chargeback_count,
        sum(magnitude_cents)                    as chargeback_cents
    from {{ ref('stg_payments__meridian_settlements') }}
    where event_type = 'chargeback'
      and not is_restated
    group by 1

),

settlement_weeks as (

    select
        week_start,
        sum(gmv_cents)                          as week_gmv_cents
    from {{ ref('settlement_weekly') }}
    group by 1

),

days as (

    select ds, dispute_status, reason_code from invoice_disputes
    union
    select ds, 'chargeback', 'card' from chargebacks

)

select
    d.ds                                        as date_key,
    d.ds,
    d.dispute_status,
    d.reason_code,

    coalesce(i.dispute_count, 0)                as invoice_dispute_count,
    coalesce(i.disputed_cents, 0)               as disputed_cents,
    coalesce(i.settled_cents, 0)                as dispute_settled_cents,
    coalesce(i.open_count, 0)                   as open_dispute_count,
    i.average_days_to_resolve,

    coalesce(c.chargeback_count, 0)             as chargeback_count,
    coalesce(c.chargeback_cents, 0)             as chargeback_cents,

    w.week_gmv_cents

from days d
left join invoice_disputes i
       on i.ds = d.ds and i.dispute_status = d.dispute_status and i.reason_code = d.reason_code
left join chargebacks c on c.ds = d.ds and d.dispute_status = 'chargeback'
left join {{ ref('dim_date') }} dd on dd.cal_date = d.ds
left join settlement_weeks w on w.week_start = dd.fiscal_week_start
