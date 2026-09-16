"""Copperline Trade's account statement, from the OMS's own export.

The desk sends every trade account a statement when the fiscal period closes:
what the account ordered in the period, in the currency it was billed in. The
figures have to be the ones the OMS published, so this reads the nightly
parquet orders export under `landing/orders_export/` rather than a mart. When
an account queries a line, the answer comes out of the file the OMS sent.

`docs/lineage.md` does not carry this job. It reads no mart, so nothing in the
lineage graph knows it is here.

One row per account, per currency, per fiscal period. The export's cents are
the order's own currency and nothing here converts them, so an account that
buys in two currencies gets two lines.

`docs/retention-policy.md` RET-2 keeps 400 days of the export live and archives
the rest, so a period older than that cannot be rebuilt from the live tree.

Owned by finance-analytics (N. Brandt).
"""

from __future__ import annotations

import datetime as dt

from include.lib import landing_dir, warehouse

__all__ = ["TABLE", "COLUMNS", "DDL", "export_glob", "period_closed",
           "statement_period", "build_statement", "publish_statement"]

#: The statement table. `marts.*` is derived, so a fresh warehouse does not
#: carry it and the build makes it before the first write.
TABLE = "marts.trade_statement"

#: The published grain, in the order both writes take it.
COLUMNS = ("period_end", "fiscal_month", "account_id", "currency_code",
           "orders", "gross_cents", "net_cents")

#: `{table}` is filled with the qualified name. `marts.*` is derived and a
#: fresh warehouse ships without it.
DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    period_end DATE NOT NULL,
    fiscal_month VARCHAR NOT NULL,
    account_id VARCHAR NOT NULL,
    currency_code VARCHAR NOT NULL,
    orders BIGINT NOT NULL,
    gross_cents BIGINT NOT NULL,
    net_cents BIGINT NOT NULL
)
"""

#: One period of the export, rolled to the statement grain.
READ = """
SELECT customer_ref                      AS account_id,
       currency_code,
       count(*)::BIGINT                  AS orders,
       sum(gross_cents)::BIGINT          AS gross_cents,
       sum(net_cents)::BIGINT            AS net_cents
FROM read_parquet(?, hive_partitioning = true)
WHERE channel = 'trade'
  AND dt BETWEEN ? AND ?
GROUP BY 1, 2
ORDER BY 1, 2
"""

#: The period a date falls in, and the days it runs between. Read from the
#: shipped 4-5-4 calendar, never computed: a fiscal period is four or five
#: weeks and is not a calendar month, whatever a particular year makes it
#: look like.
PERIOD = """
WITH here AS (
    SELECT fiscal_year, fiscal_period
    FROM {calendar}
    WHERE cal_date = ?
)
SELECT c.fiscal_year || '-P' || lpad(c.fiscal_period::VARCHAR, 2, '0'),
       min(c.cal_date),
       max(c.cal_date)
FROM {calendar} c
JOIN here h ON h.fiscal_year = c.fiscal_year
           AND h.fiscal_period = c.fiscal_period
GROUP BY 1
"""


def export_glob() -> str:
    """Every export file on disk, as one glob.

    `dt=<ds>` is the hive partition the OMS writes, one directory a day with
    one part file under it.
    """
    return str(landing_dir("orders_export") / "dt=*" / "*.parquet")


def period_closed(run_ds: str) -> bool:
    """Did a fiscal period end yesterday?

    The DAG runs on a daily cron because a 4-5-4 period closes on a Saturday
    and no monthly cron can name that morning. This is what stops the other
    thirty-odd runs a period.
    """
    last_day = dt.date.fromisoformat(run_ds) - dt.timedelta(days=1)
    with warehouse.connect(read_only=True) as con:
        _month, _first_day, period_end = statement_period(run_ds, con)
    return period_end == last_day


def statement_period(run_ds: str, con) -> tuple[str, dt.date, dt.date]:
    """The period this run reports: `(fiscal_month, first day, last day)`.

    The DAG fires the day after a period closes, so the period being reported
    is the one `ds - 1` falls in — the same convention `projects/finance/lib/
    close.py` uses for the close pack.

    Takes the caller's connection. `connect(read_only=True)` would refresh the
    snapshot from a warehouse this build is about to write, which copies the
    whole file to answer a calendar question.
    """
    last_day = dt.date.fromisoformat(run_ds) - dt.timedelta(days=1)
    row = con.execute(
        PERIOD.format(calendar=warehouse.qualify("raw.fiscal_calendar")),
        [last_day],
    ).fetchone()
    if row is None:
        raise ValueError(f"{last_day} is outside raw.fiscal_calendar")
    return str(row[0]), row[1], row[2]


def build_statement(run_ds: str, table: str = TABLE) -> int:
    """Replace one fiscal period of the statement table. Returns rows written.

    The period is the partition, keyed on the day it closed, so a rerun of the
    same close replaces its own rows and touches no other period.
    """
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('marts')}")
        con.execute(DDL.format(table=warehouse.qualify(table)))
        month, first_day, last_day = statement_period(run_ds, con)
        rows = con.execute(READ, [export_glob(), first_day, last_day]).fetchall()
        if not rows:
            raise ValueError(
                f"{month} ({first_day} to {last_day}) rolled up to nothing. "
                "Either the export is not on disk for those days or the period "
                "has aged out under RET-2 — check the landing tree before "
                "treating this as a quiet quarter."
            )
        return warehouse.delete_insert(
            table, "period_end", last_day,
            [(last_day, month, *row) for row in rows],
            columns=list(COLUMNS),
            con=con,
        )


def publish_statement(run_ds: str, table: str = TABLE) -> str:
    """Write the period out as `include/data/marts/trade_statement_<end>.csv`.

    The desk's mail merge reads the published file rather than the table.
    """
    with warehouse.connect() as con:
        month, _first_day, last_day = statement_period(run_ds, con)
        rows = con.execute(
            f"SELECT {', '.join(COLUMNS)} FROM {warehouse.qualify(table)} "
            "WHERE period_end = ? ORDER BY account_id, currency_code",
            [last_day],
        ).fetchall()
    if not rows:
        raise ValueError(f"{month}: nothing in {table} to publish")
    return str(warehouse.write_partition(
        "marts", "trade_statement", last_day, COLUMNS, rows))
