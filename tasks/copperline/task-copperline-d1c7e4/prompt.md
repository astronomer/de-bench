# MER-311 — the buyers' SKU margin is about three per cent high, and the money is on the order

Merchandising rebuild their own SKU page every Monday. They pull the week's
order lines out of the warehouse, add `line_total_cents` up by SKU, take the
stated cost off, and that is the margin they buy against. They have been running
it for a year.

Two weeks ago a buyer reconciled her department against the order book and the
page was over. Not by a rounding error — by about three per cent, on nearly
every SKU she checked, in every week she checked. Nothing on the page is wrong
in itself. The lines are right, the costs are right, the arithmetic is right. An
order is worth less than its lines say it is worth, and the page has never
carried the number that closes the gap.

Build the page in the platform, so there is one of it.

**What to write.** `projects/commerce/lib/product_margin.py`, holding

    build_day(target_ds: str, table: str) -> int

which replaces one day of `marts.product_margin_daily` and returns the rows it
wrote. One row per SKU per order date, over the orders `stg_sales__orders`
keeps and no others — everything commerce publishes starts from that view and
this page is no different. Eight columns, these names:

| column | what it holds |
|---|---|
| `ds` | the order date the row covers |
| `sku` | |
| `line_count` | order lines for that SKU on the day |
| `gross_line_cents` | the line totals the OMS wrote, tax excluded |
| `header_discount_cents` | the SKU's share of what came off the orders |
| `discounted_line_cents` | `gross_line_cents` less `header_discount_cents` |
| `landed_cost_cents` | the cost the lines stated |
| `line_margin_cents` | `discounted_line_cents` less `landed_cost_cents` |

The margin column is `line_margin_cents` and not `merch_margin_cents`, because
that name is spoken for and means something this page does not compute: this
page is order-date and does not net returns. Returns stay on the merch
dashboard.

Three things have to hold.

- The day's `discounted_line_cents`, added up over every SKU, is what the order
  book says the day was worth once the orders' own discounts are off it. To the
  cent, on any day you are given.
- How that money comes off a line is not yours to decide. The platform settled
  it once, wrote down why, and four teams read it. Do the same thing the same
  way, to the same cent. A page that lands anywhere else is a fifth number, and
  the point of this ticket is that there is one.
- Running a day twice leaves one copy of it and leaves every other day alone.

Two notes before you start. The analytics project's own models are not stood up
in this warehouse — nobody has ever built them here — so the page reads
`raw.orders` and `raw.order_lines` and does its arithmetic itself.
`marts.product_margin_daily` is new and nothing creates it yet.

Wiring it into the nightly is MER-312. This week the buyers want the days by
hand.

Then four lines in `RESPONSE.md` at the root of the working tree, for the
buyers:

- the number their page never had, named as the feed spells it
- where the rule for spreading it over the lines is written down
- what that rule is, in one sentence
- what has to be true of every single order once you have spread it
