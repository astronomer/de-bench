"""RTN-284 — returns split by the channel that sold them.

One row per order day and channel in `marts.returns_by_channel_day`: the
authorisations raised against that day's orders in that channel, the money
going back and which book it goes back into, and what the lines behind them
sold for. Store finance reads it straight.

**Two channel columns, and only one of them is the answer.** `raw.orders`
carries the channel that took the sale — `store`, `web`, `marketplace` or
`trade`. `raw.returns` carries `return_channel`, the door the goods came back
through — `mail`, `store` or `marketplace`. They are not the same column and
they are not the same vocabulary: no return ever comes back through `web`, and
14% of authorisations come back through a door that did not sell them. Reading
the door as the selling channel moves one row in seven into another channel and
leaves the company total identical, which is why nobody has caught it. The
report is read against what a channel sold, so the channel comes off the order.
`int_return_linked` takes it off the line for the same reason.

**The door has exactly one job here.** `counter_credit_cents` — the store
credit handed over a counter, whichever channel sold the goods. That is the
amount a store's own reconciliation shows against somebody else's sale, and it
is the number RTN-284 exists to settle.

**The line is a pair.** `order_line_id` is a line ordinal, `L-01` to `L-12`,
and it repeats in every order; the staging layer says so twice and
`raw.order_lines` carries the pair as its primary key. An authorisation names
(`order_id`, `order_line_id`) and nothing less.
`projects/commerce/sql/` holds four statements that join on the ordinal alone,
which is why the close's `net_sales_cents` reads negative.

**The population is the house one.** `NOT is_test`, the way `enrich_base`,
`economics_tie`, `channel_load_market` and `stg_sales__orders` all take it.
Soft-deleted orders stay in under `docs/reconciliation-policy.md` R-6.
"""

from __future__ import annotations

from include.lib import warehouse
from projects.commerce.lib import sql

__all__ = ["TABLE", "PARTITION_COL", "COLUMNS", "build_day", "tie_day"]

#: The table RTN-284 asked for, and the column a run's write is scoped to.
TABLE = "marts.returns_by_channel_day"
PARTITION_COL = "ds"

#: The order `returns_channel_build.sql` selects in.
COLUMNS = ["ds", "channel", "rmas", "returned_qty", "refund_cents",
           "card_refund_cents", "store_credit_cents", "counter_credit_cents",
           "returned_line_cents"]


def build_day(ds: str) -> int:
    """Rebuild one order day, whole, and return the number of rows written.

    Every authorisation the feed carries against that day's orders, however
    long ago it was raised. One connection: the warehouse takes one writer.
    """
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('marts')}")
        con.execute(sql.read("returns_channel_ddl"))
        rows = con.execute(sql.read("returns_channel_build"), [ds, ds]).fetchall()
        return warehouse.delete_insert(
            TABLE, PARTITION_COL, ds, rows, columns=COLUMNS, con=con,
        )


def tie_day(ds: str) -> int:
    """The day's channels hold the feed's own count for them.

    Returns the number of channels checked. A break here is a channel the build
    dropped, a channel it invented — a `mail` row is the loud case, since the
    order book never sold through that door — or a day written twice.
    """
    with warehouse.connect(read_only=True) as con:
        channels = con.execute(sql.read("returns_channel_tie"), [ds, ds]).fetchall()
    short = [row for row in channels if row[1] != row[2]]
    if short:
        first = short[0]
        raise ValueError(
            f"{ds}: {len(short)} channel(s) disagree with the returns feed. "
            f"{first[0]} holds {first[2]} authorisations and the feed carries "
            f"{first[1]}."
        )
    doubled = [row for row in channels if row[3] > 1]
    if doubled:
        first = doubled[0]
        raise ValueError(
            f"{ds}: {first[0]} holds {first[3]} rows. One row per channel."
        )
    return len(channels)
