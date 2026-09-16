"""The currency Halcyon never sent, worked out once for the whole frozen feed.

`raw.pay_halcyon_settlements` carries a `currency` column and leaves it empty on
about a third of its rows. The integration was written in 2019, when Copperline
sold in the United States only, so an empty currency and a dollar amount were the
same fact. The international launch on 2025-04-07 ended that: part of what
Halcyon settled after it is pounds and euros, landing in the same empty column
beside the dollars. `docs/billing-integration.md` B-4 is the field note.

**Where the currency comes from.** The merchant account decides it, and always
has. `raw.merchant_regions` says which market an account belongs to and
`raw.market_config` says which currency that market bills in.
`docs/runbooks/market-setup.md` MKT-1 makes the market record the system of
record for that and says why: a billing currency is a commercial decision, not a
geographical fact. Canada is the row that proves it — the Canadian market bills
USD, so a region-to-currency map written from country names puts about a tenth of
this feed under a currency it never traded in.

B-4 sends the reader to `raw.pay_merchant_accounts` for the mapping. There is no
such table. `raw.merchant_regions` is what the world holds, and
`stg_payments__merchant_regions` says the same thing in its own note.

**The amount is text.** Halcyon is the one feed in the estate that lands money as
a two-decimal string. It parses through DECIMAL rather than a float, so every
value comes back the exact cent it went out as, and it lands as integer cents —
`CONVENTIONS.md`, "Writing to the warehouse", rule 5.

**One-off, and rerun-safe.** Halcyon stopped sending on 2025-10-01 and the feed
is frozen, so this is not a dated job and has no interval to own: the run owns
the whole table. It replaces the whole table inside one transaction, so running
it twice leaves one copy of every settlement and a run that dies halfway leaves
the table as it found it.

Produces `ops.halcyon_settlement_currency`. Read by finance's conversion for the
close.

Owned by commerce.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag

from include.lib import warehouse
from include.lib.notify import notify
from include.lib.pipeline import lake_task
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("commerce", "halcyon currency backfill failed"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="halcyon_currency_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["commerce", "payments", "halcyon", "currency", "backfill"],
    doc_md=__doc__,
)
def halcyon_currency_backfill():

    @lake_task
    def backfill() -> dict:
        """Rebuild the table from the feed, in one transaction.

        The feed is frozen, so the run owns all of it and there is no day to
        scope the write to. Delete and insert together means a second run
        replaces the table rather than doubling it.
        """
        with warehouse.connect() as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS copperline.ops")
            con.execute(sql.read("halcyon_currency_ddl"))
            try:
                con.execute("BEGIN TRANSACTION")
                con.execute(
                    "DELETE FROM copperline.ops.halcyon_settlement_currency")
                con.execute(sql.read("halcyon_currency_backfill"))
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            landed = con.execute(
                "SELECT count(*) FROM copperline.ops.halcyon_settlement_currency"
            ).fetchone()[0]
        return {"rows": int(landed)}

    @lake_task
    def check_fill() -> dict:
        """Every settlement, once, under one currency per merchant account.

        Fails the run rather than reporting: finance reads this straight into
        the conversion, so a hole in it moves money.
        """
        with warehouse.connect() as con:
            missing, duplicated, unfilled, contested = con.execute(
                sql.read("halcyon_currency_check")).fetchone()
        if missing or duplicated or unfilled or contested:
            raise ValueError(
                f"halcyon currency fill: {missing} settlement(s) with no row, "
                f"{duplicated} duplicate row(s), {unfilled} row(s) with no "
                f"currency, {contested} merchant account(s) holding more than "
                "one currency")
        return {"missing": 0, "duplicated": 0, "unfilled": 0, "contested": 0}

    backfill() >> check_fill()


halcyon_currency_backfill()
