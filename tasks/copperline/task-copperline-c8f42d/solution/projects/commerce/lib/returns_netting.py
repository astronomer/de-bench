"""RTN-266 — returns netted onto the day that sold them.

One row per order day in `marts.returns_by_order_day`: the authorisations
raised against that day's orders, what they refund, and what the lines behind
them sold for. Merchandising reads it straight.

**The day is the order's, so a night's delivery scatters.** An authorisation is
raised between three and sixty days after the order it came off. Tonight's
authorisations therefore name order days spread over two months, and every one
of those days has already been published. `docs/late-data-policy.md` LD-1 says
never to drop a late row and LD-2 says to restate the day rather than append to
it, which together are this module: take the order days the night named, and
rebuild each of them whole.

**The days come from the delivery, not from a number.** A window carried over
from another job is the thing LD-1 warns about. Thirty days — the habit — holds
about half the days a night names and drops the rest silently, and the days it
drops are the older ones, where nobody is looking any more.

**The line is a pair.** `order_line_id` is a line ordinal, `L-01` to `L-12`,
and it repeats in every order; the staging layer says so twice. An
authorisation names (`order_id`, `order_line_id`) and nothing less.
`projects/commerce/sql/` holds four statements that join on the ordinal alone,
which is why the close's `net_sales_cents` reads negative.

**The population is the house one.** `NOT is_test`, the way `enrich_base`,
`economics_tie`, `channel_load_market` and `stg_sales__orders` all take it.
Soft-deleted orders stay in under `docs/reconciliation-policy.md` R-6.
"""

from __future__ import annotations

from include.lib import warehouse
from projects.commerce.lib import sql

__all__ = ["TABLE", "PARTITION_COL", "COLUMNS", "net_delivery", "tie_days"]

#: The table RTN-266 asked for, and the column a run's write is scoped to.
TABLE = "marts.returns_by_order_day"
PARTITION_COL = "ds"

#: The order `returns_netting_build.sql` selects in.
COLUMNS = ["ds", "rmas", "open_rmas", "returned_qty", "refund_cents",
           "card_refund_cents", "returned_line_cents"]


def net_delivery(ds: str) -> int:
    """Rebuild every order day one night's authorisations name.

    Returns the number of order days rewritten. One connection for the whole
    night: the warehouse takes one writer and this holds it once rather than
    sixty times.
    """
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('marts')}")
        con.execute(sql.read("returns_netting_ddl"))
        days = [row[0] for row in
                con.execute(sql.read("returns_netting_days"), [ds, ds]).fetchall()]
        for day in days:
            rows = con.execute(sql.read("returns_netting_build"), [day, day]).fetchall()
            warehouse.delete_insert(
                TABLE, PARTITION_COL, day, rows, columns=COLUMNS, con=con,
            )
    return len(days)


def tie_days(ds: str) -> int:
    """Every order day this night named holds the feed's own count for it.

    Returns the number of days checked. A break here is a day the rebuild
    missed or a day written from tonight's authorisations alone, and both are
    quiet: the table still has a row for the day and the row still looks like a
    day's worth of returns.
    """
    with warehouse.connect(read_only=True) as con:
        days = con.execute(sql.read("returns_netting_tie"), [ds, ds]).fetchall()
    short = [row for row in days if row[1] != row[2]]
    if short:
        first = short[0]
        raise ValueError(
            f"{ds}: {len(short)} order day(s) disagree with the returns feed. "
            f"{first[0]} holds {first[2]} authorisations and the feed carries "
            f"{first[1]}."
        )
    doubled = [row for row in days if row[3] != 1]
    if doubled:
        first = doubled[0]
        raise ValueError(
            f"{ds}: {first[0]} holds {first[3]} rows. One row per order day."
        )
    return len(days)
