{#-
    Tie: a recognition schedule adds up to the line it came from.

    docs/finance-policy.md REV-1 and REV-2: a line is recognised across its term
    in whole cents with the remainder on the final day. If the schedule does not
    sum to the line, revenue has been invented or lost by rounding.

    A cancelled plan stops early on purpose, so its schedule is shorter than its
    term and it is excluded.
-#}

with scheduled as (

    select
        invoice_line_key,
        sum(recognized_cents)   as scheduled_cents
    from {{ ref('int_recognition_schedule') }}
    group by 1

),

lines as (

    select invoice_line_key, line_total_cents, plan_is_cancelled
    from {{ ref('int_invoice_line_dated') }}

)

select
    l.invoice_line_key,
    l.line_total_cents,
    s.scheduled_cents,
    l.line_total_cents - s.scheduled_cents as variance_cents
from lines l
join scheduled s on s.invoice_line_key = l.invoice_line_key
where not l.plan_is_cancelled
  and l.line_total_cents <> s.scheduled_cents
