# CLS-207 — net sales in the close came back negative, and every step ran green

We reran the nightly close over a week in March to pick up a store load that
arrived late. Every run finished green — every channel count, the grain check,
the tie to the order book, all of it. `marts.order_economics` for those days now
carries a `net_sales_cents` in the billions, negative, on days we sold a few
million dollars. `booked_cents` beside it is right to the cent.

Merchandising found it, we did not, and they have RTN-266 open to build a
returns number they can argue from. This is the other half: the close's own
arithmetic, and why it went green.

The arithmetic is not wrong. Which rows meet which is wrong. **The same mistake
is in more than one statement under `projects/commerce/sql/`.** Find every one
of them. Fixing the loud one and leaving the others is how this lasted two
years.

What the fix has to hold:

- A line's net sales is what the line sold for, less what came back off **that
  line**. The returns feed says which line each authorisation came off and the
  promotion register says which line each application landed on. The shared
  models under `dbt/` say the same thing in as many words, and they are the
  reference here — read them, do not change them.
- `booked_cents` does not move. It is what `tie_to_orders` holds against
  `raw.orders` every night, it is right today, and it is the reason nobody saw
  this.
- One row per priced line, before and after.
- One of the statements you are looking for is a check. It was written to catch
  exactly this and cannot catch it. Working, it counts the authorisations in the
  night's file that answer to no order line.

Out of scope, and we mean it:

- **Do not change what the promotion push-down adds to a line.** Change which
  line it lands on and nothing else. MER-312 is settling what that statement
  should put on a line and this ticket is not getting in front of it.
- `order_economics_daily` is a separate ticket. Its statements name columns the
  shared models do not have and that job has not finished in months.
- You cannot run the close end to end here. `load_price_book` reads
  `marts.dim_product` and nothing in the tree builds it — that is CLS-208, it is
  in flight, and it is not yours. Work the statements against the warehouse by
  hand instead.
- Nothing under `dbt/`, nothing in `contracts/`, nothing in another team's
  project. Finance runs its own version of this in `projects/finance/`; we have
  told them and it is their ticket, not ours.
