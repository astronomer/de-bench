"""PAY-251 — the day's card settlement figure, by currency.

One row per settlement day per currency in `marts.card_settlement_daily`, from
the Meridian feed. The cash sheet reads it, so a day that is short here is a
day finance under-reports and nobody catches until the processor's monthly
statement lands.

**A night's delivery is not a day's settlement.** Meridian delivers hourly and
about 92% of a day's events arrive on the day they happened; the rest arrive
over the following five days — `docs/billing-integration.md` B-5. So tonight's
files carry rows for six settlement days, and five of those days have already
been published. `docs/late-data-policy.md` LD-1 says never to drop a late row
and LD-2 says to restate the day rather than append to it, which together are
this module: take the days the delivery touched, and rebuild each of them
whole.

**The days come from the delivery, not from a number.** A fixed lookback is
the thing LD-1 warns about, and three days — the number the POS catch-up and
the funnel both use, for sources that behave differently — leaves every
settlement day short of its last two days of arrivals. The loss is under one
per cent of the money, in every currency, always in the same direction, which
is exactly the size of thing a spreadsheet reconciliation never finds.

**A run reads no further than its own night.** Every row carries `loaded_at`,
the hour it reached us, and the table is the record of what each night could
have published. A rebuild that reads the whole feed makes a replayed night
report figures nobody held at the time, and the disputed-month replays stop
settling anything.
"""

from __future__ import annotations

from include.lib import warehouse
from projects.commerce.lib import sql

__all__ = ["TABLE", "PARTITION_COL", "COLUMNS", "load_delivery", "check_days"]

#: The table PAY-251 asked for, and the column a run's write is scoped to.
TABLE = "marts.card_settlement_daily"
PARTITION_COL = "ds"

#: The order `card_settlement_build.sql` selects in.
COLUMNS = ["ds", "currency_code", "events", "captured_cents", "refunded_cents",
           "chargeback_cents"]


def load_delivery(ds: str) -> int:
    """Rebuild every settlement day one night's delivery touched.

    Returns the rows written across those days. One connection for the whole
    night: the warehouse takes one writer and this holds it once rather than
    six times.
    """
    written = 0
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('marts')}")
        con.execute(sql.read("card_settlement_ddl"))
        days = [row[0] for row in
                con.execute(sql.read("card_settlement_days"), [ds, ds]).fetchall()]
        for day in days:
            rows = con.execute(
                sql.read("card_settlement_build"), [day, day, day, ds],
            ).fetchall()
            written += warehouse.delete_insert(
                TABLE, PARTITION_COL, day, rows, columns=COLUMNS, con=con,
            )
    return written


def check_days(ds: str) -> int:
    """Every day this night touched holds the feed's own count for it.

    Returns the number of days checked. A break here is a day the rebuild
    missed or a day written from the late rows alone, and both are silent in
    the totals: the money moves by well under a per cent.
    """
    with warehouse.connect(read_only=True) as con:
        days = con.execute(sql.read("card_settlement_day_tie"), [ds, ds, ds]).fetchall()
    short = [row for row in days if row[1] != row[2]]
    if short:
        first = short[0]
        raise ValueError(
            f"{ds}: {len(short)} settlement day(s) disagree with the feed. "
            f"{first[0]} holds {first[2]} events and the feed carries "
            f"{first[1]}."
        )
    split = [row for row in days if row[3] != row[4]]
    if split:
        first = split[0]
        raise ValueError(
            f"{ds}: {first[0]} holds {first[3]} rows for {first[4]} "
            "currencies. One row per currency per day."
        )
    return len(days)
