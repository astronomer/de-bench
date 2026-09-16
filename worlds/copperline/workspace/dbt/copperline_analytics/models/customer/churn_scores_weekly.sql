{{ config(materialized='table') }}

{#-
    A churn score per account, weekly.

    It is a rule, not a model. There is no training set here and nothing that
    would validate one, so the score is four stated signals with stated weights,
    and the weights are on this page where somebody can argue with them. A
    logistic regression on the same four signals would be less honest, not more
    accurate.

    `is_churned` from int_customer_lifecycle is the label the company already
    uses — no order in 180 days. The score is meant to arrive before that.
-#}

with base as (

    select
        l.party_key                             as customer_id,
        l.account_name,
        l.region_code,
        l.tier,
        l.days_since_last_order,
        l.orders_12m,
        l.orders_lifetime,
        l.net_sales_cents_12m,
        l.tenure_days,
        l.is_churned,
        l.is_one_and_done
    from {{ ref('int_customer_lifecycle') }} l
    where l.party_kind = 'trade'
      and l.orders_lifetime > 0

),

signals as (

    select
        b.*,
        coalesce(t.open_ticket_count, 0)        as open_ticket_count,
        coalesce(t.recent_tickets, 0)           as recent_tickets,
        coalesce(d.open_disputes, 0)            as open_disputes
    from base b
    left join (
        select customer_key,
               count(*) filter (where is_open) as open_ticket_count,
               count(*) filter (where opened_date > {{ ds_minus(90) }}) as recent_tickets
        from {{ ref('fct_tickets') }}
        where customer_key is not null
        group by 1
    ) t on t.customer_key = b.customer_id
    left join (
        select customer_id, open_disputes from {{ ref('customer_360') }}
    ) d on d.customer_id = b.customer_id

),

scored as (

    select
        *,
        -- Four signals, stated weights, out of 100.
        least(40, cast(days_since_last_order / 4.5 as integer))              as recency_points,
        case when orders_12m = 0 then 25 when orders_12m = 1 then 15 when orders_12m <= 3 then 8 else 0 end
                                                                             as frequency_points,
        case when open_disputes > 0 then 20 when open_ticket_count > 0 then 10 else 0 end
                                                                             as friction_points,
        case when is_one_and_done then 15 when tenure_days < 180 then 8 else 0 end
                                                                             as tenure_points
    from signals

)

select
    {{ ds() }}                                  as week_of,
    customer_id,
    account_name,
    coalesce(region_code, 'unknown')            as region,
    tier,

    days_since_last_order,
    orders_12m,
    orders_lifetime,
    net_sales_cents_12m,
    tenure_days,
    open_ticket_count,
    recent_tickets,
    open_disputes,

    recency_points,
    frequency_points,
    friction_points,
    tenure_points,
    least(100, recency_points + frequency_points + friction_points + tenure_points) as churn_score,

    case
        when least(100, recency_points + frequency_points + friction_points + tenure_points) >= 70 then 'high'
        when least(100, recency_points + frequency_points + friction_points + tenure_points) >= 40 then 'medium'
        else 'low'
    end                                         as churn_risk,

    is_churned                                  as already_churned,
    'rule: recency 40, frequency 25, friction 20, tenure 15' as scoring_note

from scored
