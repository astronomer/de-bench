{{ config(materialized='table') }}

{#-
    Retention by first-order cohort and months since.

    The cohort is the fiscal month of the first order. Months since is counted
    in fiscal periods, not in thirty-day blocks, so a cohort chart lines up with
    the close and with everything else the board reads.

    Trade accounts only, for the same reason RFM is: an identity that changes
    every visit cannot be retained.
-#}

with first_orders as (

    select
        l.party_key                             as customer_id,
        l.first_order_date,
        d.fiscal_month                          as cohort_month,
        d.fiscal_year                           as cohort_year
    from {{ ref('int_customer_lifecycle') }} l
    join {{ ref('dim_date') }} d on d.cal_date = l.first_order_date
    where l.party_kind = 'trade'

),

activity as (

    select distinct
        o.customer_ref                          as customer_id,
        d.fiscal_month                          as activity_month,
        d.fiscal_year,
        d.fiscal_period
    from {{ ref('int_orders_enriched') }} o
    join {{ ref('dim_date') }} d on d.cal_date = o.order_date
    where o.customer_ref is not null

),

cohort_months as (

    select distinct
        fiscal_month,
        fiscal_year,
        fiscal_period,
        (fiscal_year_num * 12 + fiscal_period)  as month_ordinal
    from (
        select
            fiscal_month,
            fiscal_year,
            fiscal_period,
            cast(replace(fiscal_year, 'FY', '') as integer) as fiscal_year_num
        from {{ ref('dim_date') }}
    )

),

joined as (

    select
        f.cohort_month,
        f.cohort_year,
        a.activity_month,
        cm_a.month_ordinal - cm_c.month_ordinal as months_since,
        f.customer_id
    from first_orders f
    join activity a on a.customer_id = f.customer_id
    join cohort_months cm_c on cm_c.fiscal_month = f.cohort_month
    join cohort_months cm_a on cm_a.fiscal_month = a.activity_month
    where cm_a.month_ordinal >= cm_c.month_ordinal

),

sized as (

    select cohort_month, count(distinct customer_id) as cohort_size
    from joined
    where months_since = 0
    group by 1

)

select
    j.cohort_month,
    j.cohort_year,
    j.months_since,
    s.cohort_size,
    count(distinct j.customer_id)               as active_accounts,
    cast(round(
        cast(count(distinct j.customer_id) as decimal(38, 4)) * 10000
        / nullif(s.cohort_size, 0), 0
    ) as integer)                               as retention_bps
from joined j
join sized s on s.cohort_month = j.cohort_month
group by 1, 2, 3, 4
