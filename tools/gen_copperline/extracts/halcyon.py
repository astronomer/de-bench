"""S2b — `raw.pay_halcyon_settlements`, the legacy processor's nightly CSV.

Spec 03 section 7 (S2b) and the armed register in section 21. Halcyon is the
one feed in the world that carries money as text, and the only one whose
timestamps have no zone, so most of this module is about landing those two
things badly on purpose and keeping them recoverable.

WHAT IS ARMED HERE, and how each stays recoverable:

  * **`amount` is a two-decimal string**, not integer cents. It is the one
    float-shaped money column the world ships (it and the finance workbook are
    the two exceptions). It is written by casting the decimal straight to
    `DECIMAL(18,2)` and then to text, so no float ever exists: every value
    parses back to the exact cent it came from.
  * **`currency` is NULL on 34% of rows.** Halcyon predates Copperline
    selling outside the United States, so the feed never learned a currency
    column and the one it grew is filled only when the sender bothers. The
    merchant account decides the currency and always has — `merchant_acct`
    maps one-to-one onto a currency through `raw.merchant_regions` — so the
    NULL is recoverable and never a data loss. Before E4 (2025-04-07) it is
    furniture, because every settlement was USD by construction. After E4
    about a third of the feed is GBP or EUR, so the same column that meant
    nothing in March is worth 8 to 12% of any graded total in May.
  * **`txn_datetime` is merchant-local text with no offset and no zone
    column.** The feed stops at the end of the E5 overlap quarter and never
    reaches E6, so it never gets a UTC column and never will. The zone has to
    come from the merchant account's market.
  * **`status` speaks Halcyon's vocabulary** — `SETTLED`, `REVERSED`,
    `PENDING` — which is not Meridian's. Nothing translates on the way in.

`txn_id` IS RE-MINTED. The simulation spells it from a hash into eight
digits, which collides on about one row in two thousand. A settlement feed
that repeats a transaction id is a fault nobody planted, so this module
re-mints it densely over the same `HX-########` shape. Everything else about
the row is the simulation's.

`raw.merchant_regions` IS BUILT HERE. Spec 03 section 15 files it under S10
reference data, but it exists only to give the armed NULL currency a way
home, and the account list lives in `sim_ext`. It ships one open span per
account: the six accounts have never moved market. The chapter's 62-row
target assumes an account estate the simulation does not mint, and inventing
region churn would contradict the account-to-currency map the NULL depends
on.

THE LANDING WINDOW. `landing/halcyon/dt=<ds>/settlement_<acct>.csv`, one file
per merchant account per file date, over the E5 overlap quarter and the few
days of file lag after it. That is the window PAY-207 reads and the last
window that existed when the feed stopped; the vendor's 90-day retention
(RET-1) froze there rather than rolling forward. `dt` is the file date, which
is the only load clock this feed has.
"""

from __future__ import annotations

import datetime as dt
import shutil

from ..config import Context
from ..streams import draw

# Share of rows whose `currency` arrives empty. Spec 03 section 21.
NULL_CURRENCY_PCT = 34

# Days of file lag after the feed stops that still carry a file.
FILE_LAG_DAYS = 5

LANDING_SOURCE = "halcyon"


def _merchant_regions(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.merchant_regions (
            merchant_acct VARCHAR NOT NULL,
            region_code VARCHAR NOT NULL,
            valid_from DATE NOT NULL,
            valid_to DATE,
            PRIMARY KEY (merchant_acct, valid_from)
        )
    """)
    # The accounts opened before the fixture range and never moved market.
    opened = ctx.start - dt.timedelta(days=365)
    ctx.sql(f"""
        INSERT INTO raw.merchant_regions
        SELECT merchant_acct, market_code, DATE '{opened}', NULL::DATE
        FROM sim_ext.halcyon_merchant_accounts
        ORDER BY merchant_acct
    """)


def _settlements(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.pay_halcyon_settlements (
            txn_id VARCHAR NOT NULL,
            merchant_ref VARCHAR,
            status VARCHAR,
            amount VARCHAR,
            currency VARCHAR,
            txn_datetime VARCHAR,
            settled_on DATE,
            file_date DATE,
            batch_id VARCHAR,
            merchant_acct VARCHAR
        )
    """)
    blank = draw(ctx.seed, "'halcyon_null_ccy'", "h.gateway_reference")
    ctx.sql(f"""
        INSERT INTO raw.pay_halcyon_settlements
        SELECT
            'HX-' || lpad(row_number() OVER (
                ORDER BY h.gateway_reference)::VARCHAR, 8, '0')   AS txn_id,
            h.merchant_ref,
            h.status,
            -- Two decimals as text, cast through DECIMAL so no float exists.
            h.settled_amount::DECIMAL(18,2)::VARCHAR               AS amount,
            CASE WHEN ({blank}) % 100 < {NULL_CURRENCY_PCT}
                 THEN NULL ELSE h.settlement_currency END          AS currency,
            strftime(h.txn_local_ts, '%Y-%m-%d %H:%M:%S')          AS txn_datetime,
            h.settled_on,
            h.file_date,
            h.batch_id,
            h.merchant_acct
        FROM sim_ext.halcyon_settlements h
        -- Nothing the warehouse has not been sent yet. The feed stops long
        -- before `today`, so this drops nothing; it is here so the rule is
        -- the same on all three payment feeds.
        WHERE h.file_date < DATE '{ctx.today}'
    """)


def _landing(ctx: Context) -> None:
    """One CSV per merchant account per file date, over the overlap quarter."""
    e5 = ctx.era("E5_processor_overlap")
    first = dt.date.fromisoformat(str(e5["from"]))
    last = dt.date.fromisoformat(str(e5["to"])) + dt.timedelta(days=FILE_LAG_DAYS)

    root = ctx.landing_dir(LANDING_SOURCE)
    staging = root.parent / f"_{LANDING_SOURCE}_staging"
    shutil.rmtree(staging, ignore_errors=True)
    ctx.sql(f"""
        COPY (
            SELECT file_date AS dt, merchant_acct AS acct,
                   txn_id, merchant_ref, status, amount, currency, txn_datetime,
                   settled_on, file_date, batch_id, merchant_acct
            FROM raw.pay_halcyon_settlements
            WHERE file_date BETWEEN DATE '{first}' AND DATE '{last}'
            ORDER BY file_date, merchant_acct, txn_id
        ) TO '{staging}'
        (FORMAT CSV, HEADER, PARTITION_BY (dt, acct), OVERWRITE_OR_IGNORE)
    """)
    for day_dir in sorted(staging.glob("dt=*")):
        out_day = root / day_dir.name
        out_day.mkdir(parents=True, exist_ok=True)
        for acct_dir in sorted(day_dir.glob("acct=*")):
            acct = acct_dir.name.removeprefix("acct=")
            part = next(iter(sorted(acct_dir.glob("*.csv"))))
            part.replace(out_day / f"settlement_{acct}.csv")
    shutil.rmtree(staging, ignore_errors=True)


def build(ctx: Context) -> None:
    _merchant_regions(ctx)
    _settlements(ctx)
    _landing(ctx)
