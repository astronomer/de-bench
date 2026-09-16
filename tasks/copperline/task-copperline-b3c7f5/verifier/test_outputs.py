"""CRD-141 — do the job's cents check and its publish read the mart that exists?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched DAG module the same way the scheduler
would and drives the job's own `check` and `publish` steps with the arguments
those steps carry.

**Why a fixture rather than a dbt build.** The world ships the landed half of
the warehouse only — `AGENTS.md`, "The warehouse": `staging.*` and `marts.*` are
derived and neither is on disk until the dbt build has made them. Standing
`marts.dispute_daily` up for real costs a 143-model run. `MART_SQL` below makes
it instead, out of `raw.disputes` and `raw.pay_meridian_settlements`, with the
same projection `dbt/copperline_analytics/models/commerce/dispute_daily.sql`
makes of them through `stg_finance__disputes` and
`stg_payments__meridian_settlements`. The model is protected by the task, so its
shape is fixed and this mirror stays true. The one column the mirror cannot
reach is `week_gmv_cents`, which comes off `marts.settlement_weekly` through
`dim_date`: it is here as a typed NULL, because the ticket is about the shape of
the mart and not about that number.

**No authored figures.** Every expected result is read back out of the mart the
fixture built, in the same session. The only authored things are the three days,
the mart's own column names, and each day is checked for discrimination before
it grades anything.

**Three days, on purpose.**

    2026-04-09  seven rows over four statuses and four reason codes
    2026-02-13  five rows, a different mix
    2026-05-17  five rows again, and named nowhere

Every one of them holds more rows than it holds distinct statuses, and more rows
than it holds distinct reason codes, so a publish sorted by either column on its
own leaves two rows sharing a place. All three sit outside the world's reserved
windows and outside the never-grade tail.
"""

from __future__ import annotations

import csv
import importlib
from pathlib import Path

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does —
# `DUCKDB_PATH` against the root that holds `include/`, whatever the working
# directory happens to be.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.*` under the same names, so an unqualified read
# would answer with September 2025's Northwave rows.
DISPUTES = warehouse.qualify("raw.disputes")
SETTLEMENTS = warehouse.qualify("raw.pay_meridian_settlements")
MARTS_SCHEMA = warehouse.qualify("marts")
TABLE = "marts.dispute_daily"
MART = warehouse.qualify(TABLE)

MODULE = "projects.finance.dags.fin_invoice_dispute_daily"

#: The days the two steps are measured on. See the module docstring.
FIRST = "2026-04-09"
SECOND = "2026-02-13"
THIRD = "2026-05-17"
GRADED = (FIRST, SECOND, THIRD)

#: The two money columns the mart holds for an invoice dispute, named in
#: `models/commerce/dispute_daily.sql`: `disputed_cents` is what the customer
#: claimed and `dispute_settled_cents` is what Copperline gave up. §REV-10 of
#: `docs/finance-policy.md` is the clause that separates them.
DISPUTE_MONEY = ("disputed_cents", "dispute_settled_cents")

#: The two columns that make a row of the mart unique inside a day. The model
#: groups the invoice half by `raised_date, dispute_status, reason_code` and
#: unions one chargeback row per day at `('chargeback', 'card')`.
GRAIN = ("dispute_status", "reason_code")

#: `marts.dispute_daily`, as `models/commerce/dispute_daily.sql` builds it. The
#: two staging models it reads through are views and neither joins, so both are
#: inlined here under the names the model uses.
MART_SQL = f"""
CREATE OR REPLACE TABLE {MART} AS
WITH events AS (
    SELECT * FROM {SETTLEMENTS} WHERE deleted_at IS NULL
), superseded AS (
    SELECT DISTINCT restates_event_id AS event_id
    FROM events WHERE restates_event_id IS NOT NULL
), settlements AS (
    SELECT e.event_type,
           abs(CAST(e.amount_cents AS BIGINT)) AS magnitude_cents,
           s.event_id IS NOT NULL              AS is_restated,
           e.event_time_utc,
           e.settlement_date
    FROM events e
    LEFT JOIN superseded s ON s.event_id = e.event_id
), disputes AS (
    SELECT dispute_status,
           reason_code,
           raised_on                       AS raised_date,
           CAST(disputed_cents AS BIGINT)  AS disputed_cents,
           CAST(settled_cents AS BIGINT)   AS settled_cents,
           dispute_status = 'open'         AS is_open,
           CASE WHEN resolved_on IS NOT NULL
                THEN date_diff('day', raised_on, resolved_on) END AS days_to_resolve
    FROM {DISPUTES}
), invoice_disputes AS (
    SELECT raised_date AS ds,
           dispute_status,
           reason_code,
           count(*)                        AS dispute_count,
           sum(disputed_cents)             AS disputed_cents,
           sum(settled_cents)              AS settled_cents,
           count(*) FILTER (WHERE is_open) AS open_count,
           CAST(round(avg(days_to_resolve), 0) AS INTEGER) AS average_days_to_resolve
    FROM disputes
    GROUP BY 1, 2, 3
), chargebacks AS (
    SELECT coalesce(settlement_date, CAST(event_time_utc AS DATE)) AS ds,
           count(*)            AS chargeback_count,
           sum(magnitude_cents) AS chargeback_cents
    FROM settlements
    WHERE event_type = 'chargeback' AND NOT is_restated
    GROUP BY 1
), days AS (
    SELECT ds, dispute_status, reason_code FROM invoice_disputes
    UNION
    SELECT ds, 'chargeback', 'card' FROM chargebacks
)
SELECT d.ds                                     AS date_key,
       d.ds,
       d.dispute_status,
       d.reason_code,
       coalesce(i.dispute_count, 0)::BIGINT     AS invoice_dispute_count,
       coalesce(i.disputed_cents, 0)::BIGINT    AS disputed_cents,
       coalesce(i.settled_cents, 0)::BIGINT     AS dispute_settled_cents,
       coalesce(i.open_count, 0)::BIGINT        AS open_dispute_count,
       i.average_days_to_resolve,
       coalesce(c.chargeback_count, 0)::BIGINT  AS chargeback_count,
       coalesce(c.chargeback_cents, 0)::BIGINT  AS chargeback_cents,
       CAST(NULL AS BIGINT)                     AS week_gmv_cents
FROM days d
LEFT JOIN invoice_disputes i
       ON i.ds = d.ds
      AND i.dispute_status = d.dispute_status
      AND i.reason_code = d.reason_code
LEFT JOIN chargebacks c ON c.ds = d.ds AND d.dispute_status = 'chargeback'
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection.

    DuckDB takes a single writer and the steps under test open their own, so
    nothing here may hold the file open across a call into the tree.
    """
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


@pytest.fixture(scope="session", autouse=True)
def dispute_mart():
    """`marts.dispute_daily`, as commerce's model builds it."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
        con.execute(MART_SQL)
    finally:
        con.close()


def mart_columns() -> list[str]:
    """The mart's columns, in the order the model projects them."""
    return [row[0] for row in query(f"DESCRIBE {MART}")]


def mart_rows(ds: str) -> list[tuple]:
    """One day of the mart, read straight."""
    return query(f"SELECT * FROM {MART} WHERE CAST(ds AS DATE) = CAST(? AS DATE)", [ds])


def job():
    """The patched DAG module, imported the way the scheduler imports it."""
    return importlib.import_module(MODULE)


def step(task_id: str):
    """One task of the job, with its arguments."""
    dag = getattr(job(), "dag", None)
    assert dag is not None, f"FAIL: {MODULE} no longer defines a DAG"
    task = dag.get_task(task_id)
    function = getattr(task, "python_callable", None)
    assert function is not None, (
        f"FAIL: the {task_id} step no longer calls a python function"
    )
    return function, dict(getattr(task, "op_kwargs", None) or {})


def run_step(task_id: str, ds: str):
    """Run one step for a day, with the step's own arguments.

    The arguments come off the task rather than out of this file, so a step
    whose parameter was renamed still runs: whatever the task passes is what it
    gets, and a templated argument is the day the run owns.
    """
    function, kwargs = step(task_id)
    for name, value in list(kwargs.items()):
        if isinstance(value, str) and "{{" in value:
            kwargs[name] = ds
    try:
        return function(**kwargs)
    except Exception as exc:  # noqa: BLE001 - the blame is the finding
        raise AssertionError(f"FAIL: {ds}: the {task_id} step raised {exc!r}") from exc


def checked_table() -> str:
    """The table the cents check is pointed at, as `schema.table`."""
    _, kwargs = step("check")
    table = kwargs.get("table")
    assert isinstance(table, str) and table, (
        "FAIL: the check step no longer names a table to check"
    )
    return ".".join(table.split(".")[-2:])


def checked_columns() -> list[str]:
    """The column names the cents check is handed."""
    _, kwargs = step("check")
    columns = kwargs.get("columns")
    assert isinstance(columns, (list, tuple)) and columns, (
        "FAIL: the check step no longer hands a list of money columns to "
        f"assert_integer_cents; it passes {kwargs!r}"
    )
    return [str(name) for name in columns]


def published_order() -> list[str] | None:
    """The columns the publish step sorts by, or None when it names none.

    A publish rewritten to hold its own ORDER BY does not answer here, and the
    file test falls back to reading the order out of the file itself.
    """
    _, kwargs = step("publish")
    order = kwargs.get("order_by")
    if isinstance(order, (list, tuple)) and order:
        return [str(name) for name in order]
    return None


def publish_day(ds: str) -> list[list[str]]:
    """Publish one day and return the file, header first.

    The path is the step's own answer when it gives one. A step that returns
    nothing is read at the layout `include/lib/warehouse.py` fixes and
    `docs/lineage.md` matches on: `include/data/marts/dispute_daily_<ds>.csv`.
    """
    path = run_step("publish", ds)
    file = Path(str(path)) if path else warehouse.partition_path(
        "marts", "dispute_daily", ds)
    assert file.exists(), f"FAIL: {ds}: the publish step wrote no file at {file}"
    with file.open(encoding="utf-8", newline="") as handle:
        return [row for row in csv.reader(handle)]


def grain_of(rows: list[list[str]], header: list[str]) -> list[tuple]:
    """The file's rows, cut down to the two columns that name each one."""
    missing = [name for name in GRAIN if name not in header]
    assert not missing, (
        f"FAIL: the published file has no {', '.join(missing)} column, so it is "
        "not the mart's partition"
    )
    index = [header.index(name) for name in GRAIN]
    return [tuple(row[position] for position in index) for row in rows]


def separates(ds: str, key: list[str]) -> bool:
    """Whether the key puts every row of the day in a place of its own."""
    columns = ", ".join(key)
    total, distinct = query(
        f"SELECT count(*), count(DISTINCT ({columns})) FROM {MART} "
        "WHERE CAST(ds AS DATE) = CAST(? AS DATE)",
        [ds],
    )[0]
    return total == distinct


def day_in_order(ds: str, key: list[str]) -> list[tuple]:
    """The day's rows named by grain, in the order the key sorts them.

    The sort runs in the warehouse rather than here, so a key on a money column
    is compared as a number and a key on a date as a date.
    """
    return query(
        f"SELECT {', '.join(GRAIN)} FROM {MART} "
        "WHERE CAST(ds AS DATE) = CAST(? AS DATE) "
        f"ORDER BY {', '.join(key)}",
        [ds],
    )


# ---------------------------------------------------------------- the guards

@pytest.mark.parametrize("ds", GRADED)
def test_each_graded_day_can_tell_a_whole_order_from_half_of_one(ds: str):
    """The first guard. A day only grades the publish order if it holds more
    rows than it holds distinct values of either grain column. If the world's
    dispute book ever thins out, this says so rather than passing quietly."""
    rows = mart_rows(ds)
    assert len(rows) > 1, f"FAIL: {ds}: {len(rows)} row(s), so no order is gradeable"
    columns = mart_columns()
    for name in GRAIN:
        distinct = len({row[columns.index(name)] for row in rows})
        assert distinct < len(rows), (
            f"FAIL: {ds}: {name} already separates all {len(rows)} rows, so this "
            "day cannot tell a whole order from half of one"
        )


def test_the_mart_is_one_row_per_status_and_reason_code_per_day():
    """The second guard. The publish is asked to sort a day into a fixed order,
    which is only possible because the mart's own grain does it."""
    total, distinct = query(
        f"SELECT count(*), count(DISTINCT (ds, dispute_status, reason_code)) FROM {MART}"
    )[0]
    assert total == distinct, (
        f"FAIL: {total} mart rows over {distinct} status-and-reason days, so the "
        "grain no longer separates a day"
    )


def test_the_mart_never_carried_the_columns_the_job_was_written_against():
    """The third guard. The whole ticket is that commerce's cut has neither an
    entity nor a `dispute_state` nor a `written_off_cents`. If a rebuild ever
    gives it one, this task is asking for something else."""
    assumed = ("entity", "dispute_state", "written_off_cents")
    held = mart_columns()
    found = [name for name in assumed if name in held]
    assert not found, (
        f"FAIL: the mart now carries {', '.join(found)}, so the two steps were "
        "never written against a shape that does not exist"
    )


# ----------------------------------------------------------------- the check

def test_the_cents_check_still_checks_the_dispute_mart():
    found = checked_table()
    assert found == TABLE, (
        f"FAIL: the cents check reads {found} and the job builds {TABLE}"
    )


def test_the_cents_check_names_only_columns_the_mart_carries():
    """`assert_integer_cents` passes over a name the table has not got — see
    `projects/finance/lib/warehouse_checks.py`. A check that names a column
    nobody built is a check that reports on nothing."""
    columns, held = checked_columns(), mart_columns()
    missing = [name for name in columns if name not in held]
    assert not missing, (
        f"FAIL: the cents check names {', '.join(missing)}, and {TABLE} carries "
        f"{', '.join(held)}"
    )


def test_the_cents_check_covers_the_money_a_dispute_moves():
    """The claim and the amount given up, which is what §REV-10 separates."""
    columns = set(checked_columns())
    missing = [name for name in DISPUTE_MONEY if name not in columns]
    assert not missing, (
        f"FAIL: the cents check does not look at {', '.join(missing)}"
    )


def test_the_cents_check_runs():
    """And it passes: every column it names is an integer number of cents."""
    run_step("check", FIRST)


# --------------------------------------------------------------- the publish

def test_the_publish_sorts_by_columns_the_mart_carries():
    """A publish that names a column the mart has not got raises before it
    writes a line, which is why no day has ever been published.

    A publish rewritten to hold its own ORDER BY names nothing here. It is not
    convicted for that: the day tests below read the order out of the file."""
    order = published_order()
    if order is None:
        return
    held = mart_columns()
    missing = [name for name in order if name not in held]
    assert not missing, (
        f"FAIL: the publish sorts by {', '.join(missing)}, and {TABLE} carries "
        f"{', '.join(held)}"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_the_published_day_is_the_whole_partition(ds: str):
    """The file is the mart's day, whole: the mart's columns in the mart's
    order, and one line per row of that day."""
    file = publish_day(ds)
    assert file, f"FAIL: {ds}: the published file is empty"
    header, rows = file[0], file[1:]
    assert header == mart_columns(), (
        f"FAIL: {ds}: the file's header is {header} and the mart's is {mart_columns()}"
    )
    expected = len(mart_rows(ds))
    assert len(rows) == expected, (
        f"FAIL: {ds}: the file holds {len(rows)} row(s) and the mart holds "
        f"{expected} for that day"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_the_published_day_lands_in_one_fixed_order(ds: str):
    """The ticket's second rule. Whatever the publish sorts by has to put every
    row of the day in a place of its own, or the same day published twice is two
    different files."""
    file = publish_day(ds)
    found = grain_of(file[1:], file[0])
    order = published_order()
    if order is not None:
        assert separates(ds, order), (
            f"FAIL: {ds}: {', '.join(order)} leaves two rows of the day sharing a "
            "place, so the order of the file is whatever the scan gave"
        )
        assert found == day_in_order(ds, order), (
            f"FAIL: {ds}: the file is not in the order the publish names "
            f"({', '.join(order)})"
        )
        return
    # A publish rewritten to hold its own ORDER BY still grades: the file has to
    # come out in one of the orders the mart's grain allows.
    allowed = [list(GRAIN), list(reversed(GRAIN))]
    assert any(found == day_in_order(ds, key) for key in allowed), (
        f"FAIL: {ds}: the file is not sorted into a fixed order by "
        f"{' and '.join(GRAIN)}"
    )


def test_publishing_a_day_twice_lands_the_same_file():
    """Credit control diff one day against the next, so a rerun that reorders
    the same rows is a diff full of noise."""
    once = publish_day(SECOND)
    assert publish_day(SECOND) == once, (
        f"FAIL: {SECOND}: the second publish landed a different file"
    )


def test_publishing_a_day_does_not_disturb_another_day():
    """One file per day, and a run owns the day it is given."""
    kept = publish_day(THIRD)
    publish_day(FIRST)
    assert publish_day(THIRD) == kept, (
        f"FAIL: publishing {FIRST} changed the file for {THIRD}"
    )
