"""PAY-233 — which order does a Meridian settlement event belong to.

One row per event, one order on each, into `marts.settlement_attribution`. The
refund and chargeback recovery work reads it, so the order it names decides
which store and which market the money is taken off.

**The reference on the event is not the key.** `docs/billing-integration.md`
B-6 says so and says why: `OE-#######` is seven digits over an order key that
runs past 180,000,000, so it recycles every fifty days of volume and names
about seventeen orders across the range. A settlement joined to `raw.orders` on
it matches seven of them.

**The bridge is the key.** The event carries `intent_id`, which is exact — one
intent per payment attempt — and `raw.payment_intents` is the OMS's own log of
those attempts. The attempt reaches the order on the reference and the
attempt's own `created_at`, which is the day the order was placed. That is the
part a reader has to hold on to: it is the ATTEMPT's clock that separates
orders fifty days apart, not the event's. An authorization happens the day of
the order; a refund happens six weeks after it and a chargeback later still, so
anchoring `event_time_utc` against the order date hands every refund to
whichever recycled order sits nearer. The count comes out right, the money
comes out right to the cent, and the store on the row is somebody else's.

The window is `int_orders_enriched`'s: an attempt belongs to the order it names
within a week either side. Seven days is well inside the fifty-day cadence, so
the window admits exactly one order and the match is not a guess.
"""

from __future__ import annotations

from include.lib import warehouse
from projects.commerce.lib import sql

__all__ = ["TABLE", "PARTITION_COL", "COLUMNS", "build_daily", "check_grain"]

#: The table PAY-233 asked for, and the column a run's write is scoped to.
TABLE = "marts.settlement_attribution"
PARTITION_COL = "ds"

#: The order `settlement_attribution_build.sql` selects in.
COLUMNS = ["ds", "event_id", "intent_id", "order_id", "event_type", "amount_cents"]


def build_daily(ds: str) -> int:
    """Rebuild one day of `marts.settlement_attribution`. Returns rows written.

    The day is the event's own date. `delete_insert` replaces the day and no
    other day, so a rerun and a first run leave the same table.
    """
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('marts')}")
        con.execute(sql.read("settlement_attribution_ddl"))
        rows = con.execute(sql.read("settlement_attribution_build"), [ds, ds]).fetchall()
        return warehouse.delete_insert(
            TABLE, PARTITION_COL, ds, rows, columns=COLUMNS, con=con,
        )


def check_grain(ds: str) -> int:
    """One row per event, every event, and an order on all of them.

    The feed's own count for the day is the expected count, so a join that fans
    out on the recycled reference and a join that quietly drops the refunds are
    both caught here rather than in the spreadsheet six weeks later.
    """
    with warehouse.connect(read_only=True) as con:
        rows, events, orphans, feed = con.execute(
            sql.read("settlement_attribution_grain"), [ds, ds, ds, ds],
        ).fetchone()
    if rows != events:
        raise ValueError(
            f"{ds}: {rows} rows for {events} events. One row per event — the "
            "reference is recycled and a join on it fans out."
        )
    if orphans:
        raise ValueError(f"{ds}: {orphans} rows carry no order.")
    if rows != feed:
        raise ValueError(
            f"{ds}: {rows} rows against {feed} events in the feed. Every event "
            "the feed carries for the day belongs to exactly one order."
        )
    return int(rows)
