# PRO-1140 — what the promotions actually cost, before the vendor reviews

The vendor program reviews start on the 6th. For every program we have to put a
number on the table: what that promotion took off our orders, day by day.

The number we have comes off the promotion dimension in the warehouse, and it
does not agree with the OMS. Across the last year it is short by something like
nine per cent, and short by a different amount on every promotion, so we cannot
scale it and we cannot explain it. Nobody is taking a number into a supplier
meeting that we cannot explain.

Two things.

## 1. A page we can defend

Build `marts.promo_cost_daily` from the promotion feed the OMS sends. One row
per promotion per day, for the promotions that took money that day; a promotion
that took nothing that day gets no row.

Put it in `projects/growth/lib/promo_cost.py` behind `build_daily(ds, table)` —
the date and the table name in, the number of rows written out.

Columns, in this order:

    ds, promo_id, promo_code, orders, applications, header_cents, line_cents, discount_cents

`orders` is how many of our orders took the promotion that day. `applications`
is how many times it was applied. They are not the same number.

`header_cents` is the part that came off the order and `line_cents` is the part
that came off a line. `discount_cents` is the two added.

Three rules the page holds:

- `header_cents + line_cents = discount_cents`, on every row.
- The day's `discount_cents`, added over every row, equals that day's
  order-level discounts plus its line-level discounts across the same orders,
  to the cent. That tie is what the review will be checked against.
- The build owns the day it is given. Run it twice and the day is the same.
  Run it for a second day and the first day is still there.

The order population is the one `stg_sales__orders` publishes. Read that view.
Do not invent a filter and do not leave one out.

## 2. Three lines in `RESPONSE.md`

At the root of the working tree, for the review pack:

- where the short number comes from — the model, and what that model does that
  loses the money
- which way it is wrong, and whether the money it drops is money we really gave
  away
- nothing else. It goes in front of a supplier.

## Not this week

The growth dbt models stay as they are. `docs/change-management.md` CM-2 wants
an amendment from every consumer before a shared model moves and there is no
room for that before the 6th. Leave `dbt/` alone — the page is ours to build.
