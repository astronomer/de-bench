{#-
    Tie: a carrier's monthly invoice equals what it billed on the packages.

    The freight accrual is the difference between the two, so if they never
    agree the accrual is meaningless. A tolerance of one hundred cents a
    carrier-month covers the rounding in the carrier's own totalling.

    Months with no invoice yet are excluded: that is an accrual, not a break.
-#}

select
    period_start,
    carrier_code,
    shipped_total_cents,
    invoiced_cents,
    accrual_cents
from {{ ref('freight_accrual_monthly') }}
where not not_yet_invoiced
  and abs(accrual_cents) > 100
