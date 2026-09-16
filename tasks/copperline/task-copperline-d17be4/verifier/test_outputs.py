"""CRD-140 — what `ops.dispute_open_daily` holds after the write step is rebuilt.

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched DAG module the same way the scheduler
would and drives the job's own `open_count` step.

**Why the step rather than a DAG run.** The world ships the landed half of the
warehouse only — `AGENTS.md`, "The warehouse": `staging.*` and `marts.*` are
derived and neither is on disk until the dbt build has made them. The three
steps around this one are the mart's, and the ticket says the mart is another
ticket. Driving the one step that writes the collections table grades the whole
of the ask for seconds instead of a 143-model build.

**No authored figures.** Every expected result is computed from `raw.disputes`
and `raw.invoices` in the same session. The only authored things are the three
days, and each of them is checked for discrimination before it grades anything.

**Three days, on purpose.**

    2026-03-21  a day the ticket does not name, inside the months it asks for
    2026-04-13  a second one, with a different entity taking the write-off
    2025-12-18  six months before the ticket's window, and named nowhere

All three sit outside the world's reserved windows and outside the never-grade
tail. A step that works only over the trend the ticket asks for passes the first
two and fails the third.
"""

from __future__ import annotations

import importlib
from decimal import Decimal

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
INVOICES = warehouse.qualify("raw.invoices")
OPEN_TABLE = warehouse.qualify("ops.dispute_open_daily")

MODULE = "projects.finance.dags.fin_invoice_dispute_daily"
STEP = "open_count"

#: The days the step is measured on. See the module docstring.
IN_WINDOW = "2026-03-21"
SECOND = "2026-04-13"
BEFORE = "2025-12-18"
GRADED = (IN_WINDOW, SECOND, BEFORE)

#: Every dispute, with the entity its invoice bills through. `raw.disputes`
#: carries an `owner` — `credit.03` and the like — which is the collections
#: analyst, not a reporting entity.
DISPUTE = f"""
SELECT i.entity_code AS entity,
       d.dispute_id,
       d.raised_on,
       d.resolved_on,
       d.dispute_status,
       d.disputed_cents,
       d.settled_cents
FROM {DISPUTES} d
JOIN {INVOICES} i ON i.invoice_id = d.invoice_id
"""

#: The day's position, and there is nothing behind it but the dispute record.
#:
#: Open is point-in-time: raised on or before the day, and not resolved by the
#: end of it. The write-off is §REV-10's — `settled` is the customer's favour
#: and gives up `settled_cents` on `resolved_on`; `upheld` and `rejected` are
#: Copperline's favour and give up nothing; `disputed_cents` is the claim and
#: never books.
TRUTH = f"""
WITH dispute AS ({DISPUTE})
SELECT entity,
       count(*) FILTER (
           WHERE raised_on <= CAST(? AS DATE)
             AND (resolved_on IS NULL OR resolved_on > CAST(? AS DATE))
       )::BIGINT AS open_disputes,
       coalesce(sum(disputed_cents) FILTER (
           WHERE raised_on <= CAST(? AS DATE)
             AND (resolved_on IS NULL OR resolved_on > CAST(? AS DATE))
       ), 0)::BIGINT AS disputed_cents,
       coalesce(sum(settled_cents) FILTER (
           WHERE dispute_status = 'settled' AND resolved_on = CAST(? AS DATE)
       ), 0)::BIGINT AS written_off_cents
FROM dispute
GROUP BY entity
ORDER BY entity
"""

#: What a step built on `dispute_status` produces: today's open book, the same
#: on every day it is asked for. Kept here so the graded days can be shown to
#: tell the two answers apart.
CURRENT_STATE = f"""
WITH dispute AS ({DISPUTE})
SELECT entity,
       count(*) FILTER (WHERE dispute_status = 'open')::BIGINT,
       coalesce(sum(disputed_cents) FILTER (WHERE dispute_status = 'open'), 0)::BIGINT
FROM dispute
GROUP BY entity
ORDER BY entity
"""

#: What closed on the day, split by whose favour it went. The second and third
#: figures are the two amounts a write-off could be read as.
CLOSURES = f"""
WITH dispute AS ({DISPUTE})
SELECT count(*) FILTER (WHERE dispute_status = 'settled')::BIGINT      AS settled,
       count(*) FILTER (WHERE dispute_status <> 'settled')::BIGINT     AS other,
       coalesce(sum(settled_cents) FILTER (WHERE dispute_status = 'settled'), 0)::BIGINT
                                                                       AS given_up,
       coalesce(sum(disputed_cents), 0)::BIGINT                        AS claimed
FROM dispute
WHERE resolved_on = CAST(? AS DATE)
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection.

    DuckDB takes a single writer and the step under test opens its own, so
    nothing here may hold the file open across a call into the tree.
    """
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def whole(value) -> int:
    """A money column as an integer number of cents.

    `CONVENTIONS.md`, "Writing to the warehouse": money is an integer number of
    cents at every step, so a fractional value is a defect rather than a
    rounding style and is reported as one.
    """
    if isinstance(value, (int, Decimal, float)) and value == int(value):
        return int(value)
    raise AssertionError(f"FAIL: {value!r} is not a whole number of cents")


def truth(ds: str) -> list[tuple]:
    """The day's position, with the entities that have nothing dropped."""
    rows = [(str(e), whole(n), whole(c), whole(w)) for e, n, c, w in query(TRUTH, [ds] * 5)]
    return sorted(row for row in rows if row[1] or row[2] or row[3])


def job():
    """The patched DAG module, imported the way the scheduler imports it."""
    return importlib.import_module(MODULE)


def run_step(ds: str) -> None:
    """Run the job's write step for one day, with the step's own arguments.

    The arguments come off the task rather than out of this file, so a step
    whose parameter was renamed still runs: whatever the task passes is what it
    gets, and a templated argument is the day the run owns.
    """
    dag = getattr(job(), "dag", None)
    assert dag is not None, f"FAIL: {MODULE} no longer defines a DAG"
    task = dag.get_task(STEP)
    function = getattr(task, "python_callable", None)
    assert function is not None, f"FAIL: the {STEP} step no longer calls a python function"
    kwargs = dict(getattr(task, "op_kwargs", None) or {})
    for name, value in list(kwargs.items()):
        if isinstance(value, str) and "{{" in value:
            kwargs[name] = ds
    try:
        function(**kwargs)
        return
    except TypeError:
        pass
    except Exception as exc:  # noqa: BLE001 - the blame is the finding
        raise AssertionError(f"FAIL: {ds}: the {STEP} step raised {exc!r}") from exc
    # A step rewritten to take the date positionally still runs.
    for attempt in ((ds,), ()):
        try:
            function(*attempt)
            return
        except TypeError:
            continue
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(f"FAIL: {ds}: the {STEP} step raised {exc!r}") from exc
    raise AssertionError(f"FAIL: the {STEP} step wants arguments this run cannot give it")


def written(ds: str) -> list[tuple]:
    """The day's rows, read back by name from the collections table.

    The column names are the ticket's, so a table built under other names is
    read here as a table credit control cannot query.
    """
    try:
        rows = query(
            f"SELECT entity, open_disputes, disputed_cents, written_off_cents "
            f"FROM {OPEN_TABLE} WHERE CAST(ds AS DATE) = CAST(? AS DATE)",
            [ds],
        )
    except duckdb.Error as exc:
        raise AssertionError(
            f"FAIL: {ds}: ops.dispute_open_daily does not answer for "
            f"ds, entity, open_disputes, disputed_cents, written_off_cents ({exc})"
        ) from exc
    out = [(str(e), whole(n), whole(c), whole(w)) for e, n, c, w in rows]
    keys = [row[0] for row in out]
    doubled = sorted({key for key in keys if keys.count(key) > 1})
    assert not doubled, f"FAIL: {ds}: {', '.join(doubled)} appears more than once"
    return sorted(row for row in out if row[1] or row[2] or row[3])


def check_day(ds: str) -> None:
    run_step(ds)
    found, expected = written(ds), truth(ds)
    assert found == expected, (
        f"FAIL: {ds}: the day's position is {found}, and the dispute record says {expected}"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_each_graded_day_can_tell_a_position_from_a_status(ds: str):
    """The first guard. A day only grades the point-in-time reading if the
    disputes open on it are not the disputes open now. If the world's dispute
    book ever stops moving, this says so rather than passing quietly."""
    now = query(CURRENT_STATE)
    then = [(e, n, c) for e, n, c, _ in query(TRUTH, [ds] * 5)]
    assert then != now, (
        f"FAIL: {ds}: the day's open book and this morning's are the same, so this "
        "day cannot grade the difference"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_each_graded_day_closes_disputes_both_ways(ds: str):
    """The second guard. Each day has to close at least one dispute in the
    customer's favour and at least one the other way, and the amount given up
    has to differ from the amount claimed. Without all three, a write-off read
    off the wrong status or the wrong column comes out right by accident."""
    settled, other, given_up, claimed = query(CLOSURES, [ds])[0]
    assert settled and other and given_up and given_up != claimed, (
        f"FAIL: {ds}: {settled} settled and {other} other closure(s), {given_up} cents "
        f"given up against {claimed} claimed; this day cannot grade §REV-10"
    )


def test_every_dispute_bills_through_an_entity():
    """The third guard. The entity comes off the invoice, so a dispute whose
    invoice is missing would drop out of the position without a word."""
    orphans = query(
        f"SELECT count(*) FROM {DISPUTES} d "
        f"WHERE NOT EXISTS (SELECT 1 FROM {INVOICES} i WHERE i.invoice_id = d.invoice_id)"
    )[0][0]
    assert orphans == 0, f"FAIL: {orphans} dispute(s) have no invoice to take an entity from"


def test_a_day_inside_the_months_the_ticket_asks_for():
    check_day(IN_WINDOW)


def test_a_second_day_where_a_different_entity_takes_the_write_off():
    check_day(SECOND)


def test_a_day_six_months_before_the_ticket_window():
    check_day(BEFORE)


def test_rerunning_a_day_leaves_one_copy_of_it():
    """The ticket's third rule. Credit control run the same day more than once
    while they build the trend, and a bare insert doubles it."""
    run_step(SECOND)
    once = written(SECOND)
    run_step(SECOND)
    assert written(SECOND) == once, f"FAIL: {SECOND}: the second run changed the day"


def test_a_day_does_not_disturb_another_day():
    """The partition contract. A write scoped to the table rather than to the
    day empties the trend every time the job runs."""
    run_step(BEFORE)
    kept = written(BEFORE)
    run_step(IN_WINDOW)
    assert written(BEFORE) == kept, (
        f"FAIL: building {IN_WINDOW} moved {BEFORE} from {kept} to {written(BEFORE)}"
    )
