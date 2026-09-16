{{ config(materialized='table') }}

{#-
    The freight accrual by carrier and month. **Finance reads this table.**

    It is supply's model because the shipment data is supply's, and it is on
    docs/lineage.md because `fin_accrual_freight` reads it. A change to its
    grain lands in the close pack.

    The accrual is what has shipped and not yet been invoiced: packages billed
    in the month, less the carrier invoice for that month. A month with no
    invoice yet accrues the whole thing, which is the point.
-#}

with shipped as (

    select
        date_trunc('month', p.ship_date)::date  as period_start,
        p.carrier_code,
        count(*)                                as package_count,
        count(distinct p.shipment_key)           as shipment_count,
        sum(p.billed_cents)                     as shipped_billed_cents,
        sum(p.accessorial_cents)                as shipped_accessorial_cents,
        sum(p.total_freight_cents)              as shipped_total_cents
    from {{ ref('fct_freight_billed') }} p
    group by 1, 2

),

invoiced as (

    select
        period_start,
        carrier_code,
        count(*)                                as invoice_count,
        sum(total_cents)                        as invoiced_cents,
        min(invoice_date)                       as first_invoice_date,
        max(status)                             as invoice_status
    from {{ ref('stg_logistics__carrier_invoices') }}
    group by 1, 2

),

fiscal as (

    select distinct
        date_trunc('month', cal_date)::date     as period_start,
        first_value(fiscal_month) over (
            partition by date_trunc('month', cal_date) order by cal_date
        )                                       as fiscal_month
    from {{ ref('dim_date') }}

)

select
    s.period_start,
    f.fiscal_month,
    s.carrier_code,

    s.package_count,
    s.shipment_count,
    s.shipped_billed_cents,
    s.shipped_accessorial_cents,
    s.shipped_total_cents,

    coalesce(i.invoiced_cents, 0)               as invoiced_cents,
    coalesce(i.invoice_count, 0)                as invoice_count,
    i.first_invoice_date,
    i.invoice_status,

    s.shipped_total_cents - coalesce(i.invoiced_cents, 0) as accrual_cents,
    i.period_start is null                      as not_yet_invoiced

from shipped s
left join invoiced i on i.period_start = s.period_start and i.carrier_code = s.carrier_code
left join fiscal f on f.period_start = s.period_start
