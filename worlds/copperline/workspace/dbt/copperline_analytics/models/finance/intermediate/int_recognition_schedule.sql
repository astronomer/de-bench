{{ config(materialized='view') }}

{#-
    When each invoice line's revenue is earned, month by month.

    docs/finance-policy.md REV-1 and REV-2: a line serviced over a term is
    recognised evenly across the days of the term, in whole cents, with the
    remainder on the final day. A goods line is recognised on the invoice date.

    **Whole cents, and the schedule sums to the line.** Divide a twelve-month
    agreement by its days and you get a fraction of a cent per day; recognise
    the fraction and the schedule does not add up to what was billed, which is
    the fastest way to fail a close. Every month gets its floor and the last
    month of the term gets the remainder.

    A cancelled plan stops on its cancellation date. The months after it are not
    in the schedule at all rather than being in it at zero, because a zero row
    and a missing row mean different things to the close pack.
-#}

with lines as (

    select *
    from {{ ref('int_invoice_line_dated') }}

),

months as (

    select distinct
        fiscal_month,
        min(cal_date) as month_start,
        max(cal_date) as month_end
    from {{ ref('dim_date') }}
    group by fiscal_month

),

spread as (

    select
        l.invoice_line_key,
        l.invoice_id,
        l.line_kind,
        l.entity_code,
        l.currency_key,
        l.customer_ref,
        l.account_key,
        l.billing_era,
        l.is_legacy_era,
        l.line_total_cents,
        l.service_start,
        case
            when l.plan_is_cancelled and l.plan_cancelled_on < l.service_end
            then l.plan_cancelled_on
            else l.service_end
        end                                     as effective_service_end,
        l.service_days,
        m.fiscal_month,
        m.month_start,
        m.month_end
    from lines l
    join months m
      on m.month_end >= l.service_start
     and m.month_start <= case
            when l.plan_is_cancelled and l.plan_cancelled_on < l.service_end
            then l.plan_cancelled_on
            else l.service_end
        end

),

apportioned as (

    select
        *,
        date_diff('day',
            greatest(month_start, service_start),
            least(month_end, effective_service_end)
        ) + 1                                   as days_in_month,
        row_number() over (
            partition by invoice_line_key order by month_start desc
        ) = 1                                   as is_final_month
    from spread

),

floored as (

    select
        *,
        cast(floor(
            cast(line_total_cents as decimal(38, 6)) * days_in_month
            / nullif(service_days, 0)
        ) as bigint)                            as floor_cents
    from apportioned

)

select
    {{ dbt_utils.surrogate_key(['invoice_line_key', 'fiscal_month']) }} as recognition_key,
    invoice_line_key,
    invoice_id,
    fiscal_month,
    month_start,
    month_end,
    line_kind,
    entity_code,
    currency_key                                as currency_code,
    customer_ref,
    account_key,
    billing_era,
    is_legacy_era,

    line_total_cents,
    service_start,
    effective_service_end                       as service_end,
    service_days,
    days_in_month,

    -- Every month takes its floor; the last month of the term takes whatever
    -- rounding left behind, so the schedule sums to the line exactly.
    case
        when is_final_month
        then line_total_cents - sum(floor_cents) over (partition by invoice_line_key)
             + floor_cents
        else floor_cents
    end                                         as recognized_cents,

    is_final_month

from floored
