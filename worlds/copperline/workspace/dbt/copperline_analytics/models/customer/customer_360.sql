{{ config(materialized='table') }}

{#-
    The 360. contracts/customer_360.yml, consumer C-8.

    One row per account, everything a person on a call needs. The contact is a
    hash and stays one; the service-desk export carries the hash and not the
    address. contracts/privacy.md, and this table is one of the surfaces a
    deletion request has to reach.
-#}

with customers as (

    select * from {{ ref('dim_customer') }}

),

tickets as (

    select
        customer_key,
        count(*)                                as ticket_count,
        count(*) filter (where is_open)         as open_ticket_count,
        max(opened_date)                        as last_ticket_date,
        cast(round(avg(csat), 1) as decimal(4, 1)) as average_csat
    from {{ ref('int_customer_ticket_linked') }}
    where customer_key is not null
    group by 1

),

-- Receivables and disputes come off the staging views, not off finance's
-- marts. The 360 needs the same numbers finance publishes and rule 2 says a
-- mart does not read another team's mart, so both teams read the same staged
-- invoice book and neither reads the other's output.
invoices as (

    select
        i.invoice_id,
        i.customer_ref                          as customer_key,
        i.open_cents,
        i.due_date,
        i.total_cents
    from {{ ref('stg_finance__invoices') }} i
    where i.customer_ref is not null

),

disputes as (

    select
        v.customer_key,
        count(*)                                as dispute_count,
        count(*) filter (where d.is_open)       as open_disputes,
        sum(d.disputed_cents)                   as disputed_cents
    from {{ ref('stg_finance__disputes') }} d
    join invoices v on v.invoice_id = d.invoice_id
    group by 1

),

receivables as (

    select
        customer_key,
        sum(open_cents) filter (where open_cents > 0)   as open_receivable_cents,
        count(*) filter (where open_cents > 0)          as open_invoice_count,
        max(date_diff('day', due_date, {{ ds() }})) filter (where open_cents > 0)
                                                        as worst_days_past_due
    from invoices
    group by 1

)

select
    c.customer_id,
    c.account_name,
    c.source_book,
    c.status,
    c.tier,
    c.billing_era,
    c.geography_key,
    c.region_code                               as region,
    c.country_code,

    c.first_order_date,
    c.last_order_date,
    c.orders_12m,
    c.orders_lifetime,
    c.net_sales_cents,
    c.is_churned,
    c.tenure_days,

    coalesce(d.open_disputes, 0)                as open_disputes,
    coalesce(d.dispute_count, 0)                as dispute_count,
    coalesce(d.disputed_cents, 0)               as disputed_cents,

    coalesce(r.open_receivable_cents, 0)        as open_receivable_cents,
    coalesce(r.open_invoice_count, 0)           as open_invoice_count,
    r.worst_days_past_due,

    coalesce(t.ticket_count, 0)                 as ticket_count,
    coalesce(t.open_ticket_count, 0)            as open_ticket_count,
    t.last_ticket_date,
    t.average_csat,

    c.email_hash                                as primary_contact_hash,
    c.has_undecided_merge,
    c.is_unmerged_northwave,
    {{ ds() }}::timestamp                       as updated_at

from customers c
left join tickets t on t.customer_key = c.customer_key
left join disputes d on d.customer_key = c.customer_key
left join receivables r on r.customer_key = c.customer_key
