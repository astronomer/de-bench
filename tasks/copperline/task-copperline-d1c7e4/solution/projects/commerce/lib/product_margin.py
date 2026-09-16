"""The buyers' SKU page: `marts.product_margin_daily`, one day at a time.

Merchandising kept this in a spreadsheet for a year and it read about three per
cent high, on nearly every SKU, every week. The lines were right. What the
spreadsheet never had was `raw.orders.order_discount_cents` — the promotion the
OMS applies to the order and never pushes down to a line. Add the lines up and
you have priced the order before its own discount came off.

**The discount is spread the way the platform already spreads it.** Not because
pro rata is the only defensible method, but because
`int_order_lines_discounted` settled it once, said why, and four teams read the
answer. A second method here would be a fifth number:

* pro rata by tax-exclusive line total — a discount on the order reduces what
  the customer paid for each line in proportion to what the line cost;
* integer cents, largest remainder — every line takes its floor, then the
  leftover cents go one each to the lines with the biggest fraction, so the
  shares add back to the order's discount exactly, on every order;
* an order whose lines sum to zero takes no allocation at all.

`line_margin_cents` rather than `merch_margin_cents`. That name is reserved and
means net sales less landed cost at order-line grain, and net sales is net of
returns — `docs/semantic-definitions.md`. This page is order-date and nets no
returns, so it takes a name of its own.

Owned by commerce. MER-312 hangs the nightly on it.
"""

from __future__ import annotations

from include.lib import warehouse
from projects.commerce.lib import sql

__all__ = ["COLUMNS", "build_day"]

#: The page's columns, in the order the day statement returns them, with the
#: partition column first.
COLUMNS = [
    "ds",
    "sku",
    "line_count",
    "gross_line_cents",
    "header_discount_cents",
    "discounted_line_cents",
    "landed_cost_cents",
    "line_margin_cents",
]


def build_day(target_ds: str, table: str) -> int:
    """Replace one day of the SKU page. Returns the rows written.

    One row per SKU per order date, over the orders the spine keeps: test
    orders and rows the OMS deleted are out, and nothing else is.

    The write is `delete_insert`, so the day this run owns is the day it
    replaces and no other day moves. A bare INSERT here would double the day on
    the second run, which is what `docs/late-data-policy.md` LD-2 and
    `CONVENTIONS.md` both forbid.
    """
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS copperline.marts")
        con.execute(sql.read("product_margin_mart_ddl"))
        rows = con.execute(sql.read("product_margin_day"), [target_ds]).fetchall()
        return warehouse.delete_insert(
            table,
            "ds",
            target_ds,
            [(target_ds, *row) for row in rows],
            columns=COLUMNS,
            con=con,
        )
