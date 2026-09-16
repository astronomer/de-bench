{{ config(materialized='table') }}

{#-
    The board's revenue by fiscal month, entity and account.
    contracts/account_rollup.yml, consumer C-1.

    `reported_cents` is a reserved name: recognised, net of refunds, excluding
    legacy-era plans, at fiscal-month grain — docs/semantic-definitions.md. The
    board pack excludes legacy-era plans because the contract says so; that
    exclusion is what makes `reported_cents` different from `recognized_cents`
    and it is applied here rather than in the deck.
-#}

with recognized as (

    select
        r.fiscal_month,
        r.entity_code                           as entity,
        r.customer_ref                          as customer_id,
        r.account_key,
        sum(r.recognized_cents)                 as recognized_cents,
        coalesce(sum(r.recognized_cents) filter (where not r.is_legacy_era), 0) as reported_cents,
        count(distinct r.invoice_id)            as invoice_count
    from {{ ref('int_recognition_schedule') }} r
    group by 1, 2, 3, 4

),

credits as (

    select
        m.fiscal_month,
        i.entity_code                           as entity,
        i.customer_ref                          as customer_id,
        sum(m.amount_cents)                     as credited_cents
    from {{ ref('stg_finance__credit_memos') }} m
    join {{ ref('stg_finance__invoices') }} i on i.invoice_id = m.invoice_id
    group by 1, 2, 3

),

closed as (

    select fiscal_month, is_closed, close_date from {{ ref('int_closed_month') }}

)

select
    r.fiscal_month,
    r.entity,
    coalesce(r.customer_id, 'unattributed')     as customer_id,
    r.account_key,
    a.account_name,
    a.rollup_group,

    r.recognized_cents,
    coalesce(c.credited_cents, 0)               as credited_cents,

    -- reported_cents: recognised, net of refunds, legacy-era plans removed.
    r.reported_cents - coalesce(c.credited_cents, 0) as reported_cents,

    r.invoice_count,
    count(*) over (partition by r.fiscal_month, r.entity) as active_accounts,

    cl.is_closed,
    cl.close_date

from recognized r
left join credits c
       on c.fiscal_month = r.fiscal_month
      and c.entity = r.entity
      and coalesce(c.customer_id, '~') = coalesce(r.customer_id, '~')
left join closed cl on cl.fiscal_month = r.fiscal_month
left join {{ ref('dim_account') }} a on a.account_code = r.account_key
