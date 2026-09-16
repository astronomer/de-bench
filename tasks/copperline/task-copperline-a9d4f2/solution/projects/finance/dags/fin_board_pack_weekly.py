"""The Monday board pack: revenue, the top twenty accounts, the count, the channels.

One CSV every Monday morning, read at the executive meeting the same afternoon.
Four things in it: revenue by fiscal month, the top twenty trade accounts by
reported revenue, the active account count, and the channel page for the week
that closed on Sunday.

Three things worth knowing before changing any of it.

**`reported_cents` is not `recognized_cents`.** It is recognised revenue, net of
refunds, with legacy-era plans removed, because `contracts/board-pack.md` §BP-2
excludes them. That is a presentation choice and it does not change what
recognised. `docs/semantic-definitions.md` §SD-2 keeps the names apart, and its
table says where the name lives: `marts.account_rollup`. Every page in this
deck reads that column out of that mart, so the headline and the accounts page
under it are the same measure.

**`active_accounts` is a status count, not an activity count.** An account with
no orders in a year is active if its status is active, and an account that
ordered yesterday is not active if its status is closed. §BP-1.

**The grain is binding.** The pack is monthly, and a request to move it to
another grain needs an amendment to the contract before any code changes —
§BP-3 and `docs/change-management.md` §CM-1.

Owned by finance-analytics. C-1 in `docs/report-registry.md`.
"""

from __future__ import annotations

import csv

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse, workspace_root
from include.lib.notify import notify

#: The day the pack is dated, which is the Sunday the fiscal week closed on.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

ROLLUP = "marts.account_rollup"
CUSTOMERS = "marts.dim_customer"
EXPORT_ROOT = workspace_root() / "exports" / "board_pack"

#: The channel page reads the published partitions, not the table. The pack is
#: a record of what went out, and the partitions are what went out.
CHANNEL_ROOT = workspace_root() / "include" / "data" / "marts"

#: §BP-1. Twenty rows in the companion file, and twenty is the contract.
TOP_N = 20

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify("finance", "the board pack is not built for Monday"),
}


def company_revenue(target_ds: str) -> list[list]:
    """Revenue by fiscal month and entity, as the pack shows it.

    The column the deck prints is `reported_cents` and §SD-2 says where that
    name lives: `marts.account_rollup`. Summing the account rows to fiscal
    month and entity is what makes this line and the companion page the same
    measure. It used to sum `recognized_cents` out of
    `marts.revenue_recognized_monthly` and print it under the same header,
    which is a different number by the whole legacy-era book and every credit
    memo raised against it.

    The exclusion is not recomputed here. §BP-2 puts it in the mart so that
    every reader of that mart agrees about it.
    """
    with warehouse.connect(read_only=True) as con:
        return con.execute(
            f"""SELECT fiscal_month, entity, sum(reported_cents)::BIGINT
                FROM {warehouse.qualify(ROLLUP)}
                WHERE fiscal_month <= (SELECT max(fiscal_month)
                                       FROM {warehouse.qualify(ROLLUP)}
                                       WHERE is_closed)
                GROUP BY fiscal_month, entity
                ORDER BY fiscal_month, entity"""
        ).fetchall()


def top_accounts(target_ds: str) -> list[list]:
    """The top twenty trade accounts by reported revenue, latest closed month.

    `reported_cents` already excludes legacy-era plans, per §BP-2. It is not
    recomputed here: the exclusion belongs in the mart, so the pack and any
    other reader of that mart agree about it.
    """
    with warehouse.connect(read_only=True) as con:
        return con.execute(
            f"""SELECT r.fiscal_month, r.entity, r.customer_id,
                       c.account_name, r.reported_cents
                FROM {warehouse.qualify(ROLLUP)} r
                LEFT JOIN {warehouse.qualify(CUSTOMERS)} c
                       ON c.customer_id = r.customer_id
                WHERE r.fiscal_month = (SELECT max(fiscal_month)
                                        FROM {warehouse.qualify(ROLLUP)})
                ORDER BY r.reported_cents DESC
                LIMIT {TOP_N}"""
        ).fetchall()


def active_accounts(target_ds: str) -> int:
    """Accounts whose status is active. A status count, per §BP-1.

    It is not "accounts that ordered": the pack has printed a status count
    since FY2023 and the contract says so in as many words, because the two
    numbers differ by hundreds and both are defensible if nobody has written
    down which one is meant.
    """
    with warehouse.connect(read_only=True) as con:
        return int(con.execute(
            f"SELECT count(DISTINCT customer_id) FROM {warehouse.qualify(CUSTOMERS)} "
            "WHERE account_status = 'active'"
        ).fetchone()[0])


def channel_week(target_ds: str) -> list[list]:
    """The channel page: what `channel_daily_intake` published for each day of
    the week that closed on `target_ds`, summed to day and channel.

    Reads the published partition files, not `marts.channel_daily`: the pack is
    a record of what went out, and a restated table under an already-published
    file is exactly the disagreement the page exists to surface. A day with no
    file was a day no market was due and is left off the page. A file with
    nothing under the header is a published zero, and it prints as one — the
    page trusts what was published, and which of the two a day should be is the
    intake DAG's call, not this one's.
    """
    end = pendulum.parse(target_ds)
    rows = []
    for offset in range(6, -1, -1):
        ds = end.subtract(days=offset).to_date_string()
        path = CHANNEL_ROOT / f"channel_daily_{ds}.csv"
        if not path.exists():
            continue
        by_channel: dict[str, list[int]] = {}
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                totals = by_channel.setdefault(row["channel"], [0, 0])
                totals[0] += int(row["orders"])
                totals[1] += int(row["booked_cents"])
        if not by_channel:
            rows.append([ds, "all", 0, 0])
        for channel in sorted(by_channel):
            orders, booked = by_channel[channel]
            rows.append([ds, channel, orders, booked])
    return rows


def write_channels(channels: list[list], target_ds: str) -> str:
    """Write the channel page beside the pack, one file per week."""
    EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
    page = EXPORT_ROOT / f"{target_ds}_channels.csv"
    with page.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["ds", "channel", "orders", "booked_cents"])
        writer.writerows(channels)
    return str(page)


def write_pack(revenue: list[list], accounts: list[list], count: int,
               target_ds: str) -> list[str]:
    """Write the pack and its companion file, and return both paths."""
    EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
    pack = EXPORT_ROOT / f"{target_ds}.csv"
    companion = EXPORT_ROOT / f"{target_ds}_accounts.csv"

    with pack.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["fiscal_month", "entity", "reported_cents",
                         "active_accounts"])
        for fiscal_month, entity, cents in revenue:
            writer.writerow([fiscal_month, entity, cents, count])

    with companion.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["fiscal_month", "entity", "customer_id", "account_name",
                         "reported_cents"])
        writer.writerows(accounts)

    return [str(pack), str(companion)]


def verify_grain(revenue: list[list]) -> int:
    """One row per fiscal month per entity, and no month twice. §BP-3.

    The grain is the contract. A duplicated month is what a join to a table
    that has moved to a finer grain does, and the pack would print the same
    month twice with two different totals.
    """
    keys = [(row[0], row[1]) for row in revenue]
    if len(keys) != len(set(keys)):
        raise ValueError("the pack holds a fiscal month and entity twice")
    return len(keys)


def verify_top_n(accounts: list[list]) -> int:
    """Twenty rows, or as many as the book holds if it holds fewer."""
    if len(accounts) > TOP_N:
        raise ValueError(f"{len(accounts)} accounts and the contract says {TOP_N}")
    return len(accounts)


with DAG(
    dag_id="fin_board_pack_weekly",
    schedule="0 9 * * 1",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "board", "weekly"],
    doc_md=__doc__,
) as dag:
    revenue = PythonOperator(
        task_id="company_revenue",
        python_callable=company_revenue,
        op_kwargs={"target_ds": TARGET_DS},
    )

    accounts = PythonOperator(
        task_id="top_accounts",
        python_callable=top_accounts,
        op_kwargs={"target_ds": TARGET_DS},
    )

    count = PythonOperator(
        task_id="active_accounts",
        python_callable=active_accounts,
        op_kwargs={"target_ds": TARGET_DS},
    )

    check_grain = PythonOperator(
        task_id="verify_grain",
        python_callable=verify_grain,
        op_kwargs={"revenue": revenue.output},
    )

    check_top = PythonOperator(
        task_id="verify_top_n",
        python_callable=verify_top_n,
        op_kwargs={"accounts": accounts.output},
    )

    channels = PythonOperator(
        task_id="channel_week",
        python_callable=channel_week,
        op_kwargs={"target_ds": TARGET_DS},
    )

    channel_page = PythonOperator(
        task_id="write_channels",
        python_callable=write_channels,
        op_kwargs={"channels": channels.output, "target_ds": TARGET_DS},
    )

    write = PythonOperator(
        task_id="write_pack",
        python_callable=write_pack,
        op_kwargs={"revenue": revenue.output, "accounts": accounts.output,
                   "count": count.output, "target_ds": TARGET_DS},
    )

    [check_grain, check_top, count] >> write
    revenue >> check_grain
    accounts >> check_top
    channels >> channel_page
