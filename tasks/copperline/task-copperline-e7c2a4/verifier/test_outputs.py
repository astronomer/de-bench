"""PAY-249 — does the matching step finally pair an order with its payment?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched DAG module the same way Airflow does.

**Why a verifier rather than a DAG run.** `payments_recon_daily` cannot reach
its last step in this warehouse: the world ships the landed half only
(`AGENTS.md`, "The warehouse") and nothing in the tree creates the ladder's
working tables, or even the `staging` schema that would hold them — the
fixture below makes that schema and nothing else. The subject is the first
step, and calling it directly grades the same code in seconds. It also removes
the shortcut — a day typed into
`staging.recon_matched` by hand is a day the next call replaces before anything
reads it.

**No authored numbers.** Every expected result is computed from `raw.orders`,
`raw.payment_intents` and `raw.pay_meridian_settlements` in the same session.
All three are read-only landing tables, and a trial's patch carries nothing
under `include/data` at all, so neither a tree edit nor a warehouse edit can
move the answer.

**Four days, and the ticket names none of them.**

    2025-11-12   Meridian authoritative, ordinary volume
    2026-01-08   the same, two months on
    2026-04-22   the same, five months on
    2025-03-11   before Meridian took any traffic, so the honest match and the
                 shipped one agree to the row. Nothing may move here.

None of the four is a closure day or a spike, none is named by any other
ticket, and the first three sit well clear of the processor overlap quarter.

**What the four queries below are for.** `TRUTH` is the answer. `PSP_KEY` is
what the tree ships, `STRIPPED` is the repair the shipped join invites, and
`REF_ONLY` is the link the reference alone gives. The first test proves each
graded day can still tell them apart, so a world that stops recycling the
reference fails here rather than passing a task that measures nothing.
"""

from __future__ import annotations

import traceback

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the way every DAG in the world does —
# `DUCKDB_PATH` against the root that holds `include/`, whatever the working
# directory happens to be.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.orders` under the same name, so an unqualified
# read would answer with September 2025's Northwave numbers.
ORDERS = warehouse.qualify("raw.orders")
INTENTS = warehouse.qualify("raw.payment_intents")
EVENTS = warehouse.qualify("raw.pay_meridian_settlements")
TARGET = warehouse.qualify("staging.recon_matched")
STAGING = warehouse.qualify("staging")

#: The days the step is measured on. See the module docstring.
GRADED = ("2025-11-12", "2026-01-08", "2026-04-22")
QUIET = "2025-03-11"

#: The columns the working table has always carried, in the order everything
#: here compares them in. A step that carries more than these is not convicted
#: for it; one missing any of them fails to read at all.
COLUMNS = (
    "order_id, order_cents, order_status, event_id, event_type, "
    "processor_cents, event_time_utc, deleted_at_source"
)

#: What the day's rows have to be. An order reaches its payment attempt on the
#: reference plus the attempt's own clock — the reference recycles about every
#: fifty days and an attempt is raised a day or two after its order, so a week
#: either side admits exactly one order. That is `int_orders_enriched`'s rule.
#: `intent_id` carries the attempt the rest of the way to the settlement. Both
#: joins are LEFT: an order the processor never saw is R-7's `no_payment` and
#: has to survive this step.
TRUTH = f"""
SELECT o.order_id, o.grand_total_cents, o.order_status,
       s.event_id, s.event_type, s.amount_cents, s.event_time_utc,
       o.deleted_at IS NOT NULL
FROM {ORDERS} o
LEFT JOIN {INTENTS} i
       ON i.order_ref = o.order_ref
      AND i.created_at::DATE BETWEEN o.local_order_date - 7
                                 AND o.local_order_date + 7
LEFT JOIN {EVENTS} s
       ON s.intent_id = i.intent_id
      AND s.attempt_no = i.attempt_no
      AND s.deleted_at IS NULL
WHERE o.local_order_date = ?
"""

#: What the tree ships. `raw.payment_intents.order_id` is the processor's own
#: counter; ours are `ORD-########`, so the cast is NULL on every row and the
#: join matches nothing at all.
PSP_KEY = TRUTH.replace(
    """ON i.order_ref = o.order_ref
      AND i.created_at::DATE BETWEEN o.local_order_date - 7
                                 AND o.local_order_date + 7""",
    "ON i.order_id = try_cast(o.order_id AS BIGINT)",
)

#: The repair the shipped join invites: take the digits out of `ORD-########`
#: and cast those instead. The two sets of numbers do not overlap anywhere, so
#: this matches nothing either — see checks.yaml for the proof.
STRIPPED = TRUTH.replace(
    """ON i.order_ref = o.order_ref
      AND i.created_at::DATE BETWEEN o.local_order_date - 7
                                 AND o.local_order_date + 7""",
    "ON i.order_id = try_cast(substr(o.order_id, 5) AS BIGINT)",
)

#: The reference on its own, with no clock. It matches, and it matches several
#: orders per attempt, because the OMS recycles the reference.
REF_ONLY = TRUTH.replace(
    """AND i.created_at::DATE BETWEEN o.local_order_date - 7
                                 AND o.local_order_date + 7""",
    "",
)

ORDER_COUNT = f"SELECT count(*) FROM {ORDERS} WHERE local_order_date = ?"


@pytest.fixture(scope="session", autouse=True)
def staging_schema():
    """Make `staging` before anything runs.

    The world ships the landed half of the warehouse only — `AGENTS.md`, "The
    warehouse": `staging.*` and `marts.*` are derived, so the file carries
    neither the tables nor the schemas that hold them, and the runs make them.
    The step under test creates its own table and needs the schema to be there,
    so this makes it once. It is scenery, not part of the answer: a fix that
    also creates the schema finds it there already and passes just the same.
    """
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING}")
    finally:
        con.close()


def require(ok: bool, why: str) -> None:
    """Fail with a reason the report can read.

    The reason is PRINTED as well as raised. The scorer runs pytest with
    `--tb=line` and lifts the blame out of the run's own output by looking for
    lines that begin `FAIL: `; an assertion message alone reaches the log
    prefixed with pytest's `E`, and never reaches the report.
    """
    if not ok:
        print(why)
        raise AssertionError(why)


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the step
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def ordered(rows) -> list[tuple]:
    """Rows in one order, NULLs last. A list rather than a set, so a step that
    writes a row twice is caught by the comparison and not only by the count."""
    return sorted(
        (tuple(r) for r in rows), key=lambda r: [(c is None, str(c)) for c in r]
    )


def expected(ds: str) -> list[tuple]:
    return ordered(query(TRUTH, [ds]))


def match(ds: str) -> int:
    """Run the patched step for one day, the way the DAG's first task does.

    Anything that goes wrong inside it is re-raised as an assertion, so the
    report carries the reason on one line instead of only a test name.
    """
    try:
        from projects.commerce.dags import payments_recon_daily

        return payments_recon_daily.match_payments(ds)
    except Exception as exc:  # noqa: BLE001 - the reason is the finding
        # The whole traceback goes to the run's log; the report only carries
        # the first 160 characters of a `FAIL: ` line, so that line leads with
        # the exception and nothing else.
        traceback.print_exc()
        why = str(exc).replace("\n", " ")
        require(False, f"FAIL: {ds}: {type(exc).__name__} from match_payments: {why}")
        raise AssertionError from exc  # unreachable; keeps the type checkers happy


def built(ds: str) -> list[tuple]:
    return ordered(query(f"SELECT {COLUMNS} FROM {TARGET} WHERE ds = ?", [ds]))


def blame(ds: str, actual: list[tuple], want: list[tuple]) -> str:
    extra = sorted(set(actual) - set(want))[:3]
    lost = sorted(set(want) - set(actual))[:3]
    return (
        f"FAIL: {ds}: the step wrote {len(actual)} rows and the orders and the "
        f"feed give {len(want)}. {len(set(actual) - set(want))} rows the two "
        f"sides do not support and {len(set(want) - set(actual))} of theirs "
        f"missing. first wrong {extra}; first missing {lost}"
    )


def check_day(ds: str) -> None:
    want = expected(ds)
    require(bool(want), f"FAIL: {ds}: no orders that day, so nothing is graded")
    match(ds)
    actual = built(ds)
    # The comparison is made before the call on purpose: handing pytest two
    # lists of thousands of rows makes it print a diff of all of them, and the
    # blame line says everything the report can use.
    require(actual == want, blame(ds, actual, want))


def test_each_graded_day_can_tell_the_answers_apart():
    """The fixture's own guard.

    A graded day is only worth grading if the honest link differs on it from
    the shipped join, from the repair the shipped join invites, and from the
    reference taken on its own. If the world ever stops recycling the reference
    or stops retrying payments, this fails here rather than passing a task that
    measures nothing.
    """
    for ds in GRADED:
        want = expected(ds)
        paid = [r for r in want if r[3] is not None]
        require(bool(paid), f"FAIL: {ds}: the honest link finds no payment at all")
        for name, sql in (("shipped", PSP_KEY), ("digits-only", STRIPPED)):
            hits = [r for r in query(sql, [ds]) if r[3] is not None]
            require(
                not hits,
                f"FAIL: {ds}: the {name} join already matches {len(hits)} "
                "events, so this day cannot grade the fix",
            )
        require(
            len(query(REF_ONLY, [ds])) > len(want),
            f"FAIL: {ds}: the reference does not fan out here, so this day "
            "cannot grade the window",
        )


def test_the_first_graded_day_pairs_every_order_with_its_payment():
    check_day(GRADED[0])


def test_the_second_graded_day_pairs_every_order_with_its_payment():
    check_day(GRADED[1])


def test_the_third_graded_day_pairs_every_order_with_its_payment():
    check_day(GRADED[2])


def test_a_day_before_the_processor_existed_is_left_exactly_as_it_was():
    """Nothing may move on a day with no Meridian traffic behind it.

    The honest link and the shipped one give the same rows here — every order
    once, a payment on none of them. A step that drops the orders with no
    event, or that widens the grain to make the numbers move, fails here while
    passing nothing else.
    """
    check_day(QUIET)


def test_every_order_of_the_day_is_still_in_the_match():
    """R-7 is read off this table. `no_payment` is an order with no event, so
    an order the processor never saw has to survive the step — which is why
    both joins are LEFT. This also fixes the size of the exception list: the
    rows with no event must be only the orders the processor genuinely never
    saw, not the whole order book."""
    ds = GRADED[1]
    match(ds)
    orders = query(ORDER_COUNT, [ds])[0][0]
    rows = query(
        f"SELECT count(*), count(DISTINCT order_id), "
        f"count(*) FILTER (WHERE event_id IS NULL) FROM {TARGET} WHERE ds = ?",
        [ds],
    )[0]
    want_eventless = len([r for r in expected(ds) if r[3] is None])
    require(
        rows[1] == orders,
        f"FAIL: {ds}: {rows[1]} orders in the match against {orders} orders "
        "that day. Every order of the day belongs in it once, paid or not",
    )
    require(
        rows[2] == want_eventless,
        f"FAIL: {ds}: {rows[2]} rows carry no processor event and the feed "
        f"supports {want_eventless}. R-7 files exactly these as no_payment",
    )
    require(
        rows[2] < orders,
        f"FAIL: {ds}: all {orders} orders still carry no processor event",
    )


def test_the_step_reports_the_rows_it_wrote():
    """The DAG's log is the only place a short day shows before the squad's
    morning list does."""
    ds = GRADED[2]
    written = match(ds)
    held = len(built(ds))
    require(
        written == held,
        f"FAIL: {ds}: the step reported {written} rows and the table holds {held}",
    )


def test_running_the_day_twice_leaves_one_copy():
    """The step is rerun on every retry and on every backfill."""
    ds = GRADED[0]
    match(ds)
    once = built(ds)
    match(ds)
    require(built(ds) == once, f"FAIL: {ds}: the second run changed the day")
