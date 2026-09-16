# DQ-96 — the store tie, measured

`tie_channel_daily_store_pos` fails because the two sides of it are not the same quantity.
The till side carries tax and the order side does not. Everything else in the gap is small
beside that.

## The gap

Over the whole range, on the store-days that carry both sides:

| | |
|---|---:|
| store-days with both sides | 213,408 |
| store-days the tie fails on | 213,232 |
| gap, till less order book | **+7,389,678,723 cents** |

Signed the way `variance_cents` signs it: `till_net_cents - (booked_cents -
order_discount_cents)`. The till is ahead by about $73.9m. 176 store-days out of 213,408
come in under the hundred-cent allowance, so this is not a tail of bad nights — it is every
night we have.

## The three reasons the model header gives

| reason from the header | cents into the gap |
|---|---:|
| a store transaction voided after the batch closed | **-1,596,054,634** |
| the 23:05 store-local trading day against the local order date | **0** |
| the pre-cutover era with no UTC stamp to line the two up | **0** |

**Voids: -1,596,054,634 cents, over 33,463 voided transactions on 28,877 store-days.**
`agg_daily_store_sales` sums the till with `filter (where not is_void)` and the order book
keeps the order, so a void leaves one side and not the other. This one is real and it is
the only one of the three that is.

**The trading day: nothing.** The two dates are the same date. The till groups on
`raw.pos_sales_header.business_date` and the order book on `raw.orders.local_order_date`,
and those two columns are equal on all 1,776,598 till transactions that carry an order id —
before the cutover and after it. The 23:05 close batch is a real thing about this feed, and
STO-419 turns on it, but it only bites a reader who converts a stamp into a day. Neither
side of this comparison converts anything: both read a date column that was already the
store's own trading day.

**The pre-cutover era: nothing.** Neither side of the comparison reads `event_time_utc`.
The column is NULL on every store transaction before 2025-11-03, which is true and is not
in this arithmetic. `stg_store__pos_sales_header` passes it through and
`agg_daily_store_sales` never selects it.

## What the header does not say

| cause | cents into the gap |
|---|---:|
| tax, on the till side only | **+8,390,741,710** |
| orders the till has and the order spine drops | **+594,991,647** |

**Tax: +8,390,741,710 cents.** `raw.pos_sales_header.net_cents` is `gross_cents -
discount_cents + tax_cents`. The order side of the comparison is `booked_cents -
order_discount_cents`, and `booked_cents` is `subtotal_cents`, which is before tax.
`fct_order` publishes `tax_cents` right beside it and the tie does not use it. This is on
every store-day that traded, which is why the tie fails on all of them.

**Dropped orders: +594,991,647 cents, over 12,481 orders on 11,532 store-days.**
`stg_sales__orders` drops `is_test` orders and rows with a `deleted_at`, and says so in its
own header. The registers sent both — the POS feed is what the till rang up, not what the
OMS kept. That is 9,659 test orders worth 459,552,258 cents and 2,822 soft-deleted ones
worth 135,439,389 cents.

## The arithmetic

    +8,390,741,710   tax the till carries and the order side does not
    -1,596,054,634   voided transactions the till drops and the order book keeps
      +594,991,647   test and soft-deleted orders the till has and the spine drops
    ==============
    +7,389,678,723   the gap

Exact, not approximate. There is no fourth term.

## What an honest tie would compare

Put the two sides on the same footing and there is a test worth keeping:

- Compare like with like on tax — either `till_net_cents` against `booked_cents -
  order_discount_cents + tax_cents`, or both sides before tax, taking the till as
  `gross_cents - discount_cents`. Either way, one quantity on both sides.
- Take the voided transactions off the order side too, or leave them on both. A void is a
  transaction that happened and took no money, and the tie should not be the place we find
  that out.
- Compare the same population. Either the tie reads the order book before the test and
  soft-deleted rows are dropped, or it accepts that it is comparing the till against a
  filtered book and says which filter.

That tie would still go red for the things worth a page at 04:00: a store whose batch never
came, a partial file that loaded short, a late batch counted on the wrong night, a register
that double-rang a day. It would report a handful of store-days a night instead of all of
them, which is the difference between a test and a wall.

Nothing under `dbt/` has been changed. The tie is still red and the nightly is still red.
