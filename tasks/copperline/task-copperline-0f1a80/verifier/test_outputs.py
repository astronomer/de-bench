"""CUS-537 — does the weekly churn job score a week that is not this week, and
does it write it under a name of its own?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way a DAG would.

**Why a verifier rather than a DAG run.** The world ships the landed half of the
warehouse only (`AGENTS.md`, "The warehouse"): `raw.*` and `ops.load_control`
are on disk and every `marts.*` model is derived, so `marts.customer_360` — the
table the feature step reads — does not exist until a 143-model dbt build has
made it. Standing that up costs half an hour for one weighted sum. The fixture
below makes the three relations the job reads and writes, out of `raw.*`, and
calls the patched module directly. It also removes the second reason a run would
say nothing: a trial's patch never carries anything under `include/data`, so a
week an agent scored by hand is a week no check can read.

**What the fixture stands in for.**

`marts.customer_360` is made from `raw.customers`, `raw.orders`,
`raw.customer_id_map`, `raw.disputes` and `raw.invoices`, with the columns
`build_features` reads and the projection the model makes of them for those
columns: the order spine filtered the way staging filters it, the pre-cutover
account refs resolved through the crosswalk the way
`docs/runbooks/customer-id-migration.md` says to resolve them, the twelve-month
window measured from the pinned warehouse clock the way `int_customer_lifecycle`
measures it, and `churned` mapped to `closed` the way `dim_customer` maps it.
It is a stand-in for those columns and not a second copy of the model.

`ops.churn_features` is created empty, in the shape the feature step writes
today. No file in the workspace creates it and no generator does — that is
estate-wide and it is not what this ticket moves.

`marts.churn_scores_weekly` is created empty, in the shape the nightly dbt
model leaves behind: `week_of` and no `ds`, which is the collision the ticket is
about. Nothing here writes a row into it. The test that matters is that nothing
in the patched tree writes one either.

**No authored figures.** Every expected score is computed from the fixture's own
rows, in Python, part by part — the module's SQL and this answer key share no
code path. The five weights are the ones `WEIGHTS` has always carried and the
ticket does not move them, so they are written down here and checked against the
module rather than read from it.

**Three weeks, on purpose.**

    2026-04-10  inside the twelve weeks the ticket names, and a Friday it does
                not name
    2026-02-20  four weeks before that window opens
    2025-11-14  seven months before it, and named nowhere

A repair that special-cases the ticket's window passes the first and fails the
other two. All three sit outside the world's reserved windows and outside the
never-grade tail, and all three carry accounts whose last order falls after the
week — `test_the_graded_weeks_can_tell_the_answers_apart` measures that rather
than trusting this paragraph.

**The tolerance.** `SCORE_TOLERANCE` is one ten-thousandth, because the module
rounds the weighted sum to four places and this file does not round at all. The
reading the ticket is about — a part left outside its unit — is worth up to a
third of the score, which is three orders of magnitude away, and the guard test
measures that gap every run.
"""

from __future__ import annotations

import csv
import datetime as dt
from functools import lru_cache

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
# search path and carries tables under the same names, so an unqualified read
# inside a view would answer with September 2025's Northwave numbers.
MARTS = warehouse.qualify("marts")
OPS = warehouse.qualify("ops")
C360 = warehouse.qualify("marts.customer_360")
FEATURES = warehouse.qualify("ops.churn_features")
MODEL_TABLE = warehouse.qualify("marts.churn_scores_weekly")

#: The table the ticket asks this job to write, and the columns it asks for.
JOB_TABLE_NAME = "marts.customer_churn_weekly"
JOB_TABLE = warehouse.qualify(JOB_TABLE_NAME)
JOB_COLUMNS = ("customer_id", "source_book", "churn_score", "score_scale",
               "history_days", "ds")

#: The literal the ticket puts on every row.
SCALE = "0-1"

#: The weeks the fix is measured on. See the module docstring.
IN_WINDOW = dt.date(2026, 4, 10)
BEFORE = dt.date(2026, 2, 20)
EARLIER = dt.date(2025, 11, 14)
GRADED = (IN_WINDOW, BEFORE, EARLIER)

#: A week nothing grades, used to hand the guard a row it must object to. It
#: sits in the world's reserved window, where no shipped check reads a date.
SCRATCH = dt.date(2026, 6, 3)

#: The pinned warehouse clock. `dbt_project.yml` sets `var('ds')` from
#: `WORLD_TODAY` and `int_customer_lifecycle` measures its windows against it,
#: so the fixture's twelve-month count is measured against it too.
WORLD_TODAY = dt.date(2026, 6, 15)

#: The model, as `WEIGHTS` has always stated it. The ticket does not move it.
EXPECTED_WEIGHTS = {
    "recency": 0.35,
    "frequency": 0.25,
    "value": 0.15,
    "support_load": 0.15,
    "open_dispute": 0.10,
}

#: See "The tolerance" in the module docstring.
SCORE_TOLERANCE = 1e-4

#: The columns the nightly dbt model selects, as far as this fixture needs to
#: stand in for it: the key, the score out of a hundred, the band, and the week
#: it is keyed on. There is no `ds` here and that is the point.
MODEL_DDL = f"""
CREATE TABLE IF NOT EXISTS {MODEL_TABLE} (
    week_of         DATE,
    customer_id     VARCHAR,
    churn_score     INTEGER,
    churn_risk      VARCHAR,
    scoring_note    VARCHAR
)
"""

#: The features table, in the shape the feature step writes today.
FEATURES_DDL = f"""
CREATE TABLE IF NOT EXISTS {FEATURES} (
    customer_id     VARCHAR,
    source_book     VARCHAR,
    recency         DOUBLE,
    frequency       DOUBLE,
    value           DOUBLE,
    support_load    DOUBLE,
    open_dispute    DOUBLE,
    history_days    BIGINT,
    ds              DATE
)
"""

#: The 360, for the columns the feature step reads. See the module docstring.
C360_DDL = f"""
CREATE OR REPLACE TABLE {C360} AS
WITH kept AS (
    SELECT coalesce(m.customer_id, o.customer_ref)  AS customer_id,
           o.local_order_date,
           o.subtotal_cents
    FROM {warehouse.qualify('raw.orders')} o
    LEFT JOIN {warehouse.qualify('raw.customer_id_map')} m
           ON m.legacy_id = o.customer_ref
    WHERE o.customer_ref IS NOT NULL
      AND NOT coalesce(o.is_test, false)
      AND o.deleted_at IS NULL
),
spine AS (
    SELECT customer_id,
           min(local_order_date)                    AS first_order_date,
           max(local_order_date)                    AS last_order_date,
           count(*) FILTER (local_order_date > DATE '{WORLD_TODAY}' - INTERVAL 365 DAY)
                                                    AS orders_12m,
           sum(subtotal_cents)                      AS net_sales_cents
    FROM kept
    GROUP BY 1
),
open_disputes AS (
    SELECT i.customer_ref                           AS customer_id,
           count(*)                                 AS open_disputes
    FROM {warehouse.qualify('raw.disputes')} d
    JOIN {warehouse.qualify('raw.invoices')} i ON i.invoice_id = d.invoice_id
    WHERE d.dispute_status = 'open' AND i.customer_ref IS NOT NULL
    GROUP BY 1
)
SELECT c.customer_id,
       'copperline'                                 AS source_book,
       CASE WHEN c.status = 'churned' THEN 'closed' ELSE c.status END AS status,
       s.first_order_date,
       s.last_order_date,
       coalesce(s.orders_12m, 0)                    AS orders_12m,
       coalesce(s.net_sales_cents, 0)               AS net_sales_cents,
       coalesce(p.open_disputes, 0)                 AS open_disputes
FROM {warehouse.qualify('raw.customers')} c
LEFT JOIN spine s ON s.customer_id = c.customer_id
LEFT JOIN open_disputes p ON p.customer_id = c.customer_id
WHERE c.deleted_at IS NULL
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the module
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def statement(sql: str, params: list | None = None) -> None:
    con = duckdb.connect(DB)
    try:
        con.execute(sql, params or [])
    finally:
        con.close()


@pytest.fixture(scope="session", autouse=True)
def warehouse_ready():
    """The three relations the job reads and writes. See the module docstring."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS}")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {OPS}")
        con.execute(C360_DDL)
        con.execute(FEATURES_DDL)
        con.execute(MODEL_DDL)
    finally:
        con.close()


# --------------------------------------------------------------------------
# The expected scores, from the fixture's own rows
# --------------------------------------------------------------------------

def bounded(value: float) -> float:
    """A part, held inside zero-to-one, which is what the module says of its
    own parts."""
    return max(0.0, min(1.0, value))


@lru_cache(maxsize=1)
def population() -> list[tuple]:
    """The accounts the feature step holds: the 360 less the closed ones."""
    rows = query(
        f"SELECT customer_id, last_order_date, orders_12m, net_sales_cents, "
        f"open_disputes FROM {C360} WHERE status <> 'closed' ORDER BY customer_id"
    )
    assert rows, "FAIL: the fixture built an empty 360, so nothing is graded"
    return rows


@lru_cache(maxsize=None)
def expected(day: dt.date) -> dict[str, float]:
    """The week's score per account, part by part.

    The five parts are the ones the module has always named: days since the
    last order over a year, orders in twelve months against twenty-four, money
    against fifty thousand, open disputes against three, and whether there is a
    dispute at all. Each is held inside zero-to-one before it is weighted.
    """
    out: dict[str, float] = {}
    for customer, last_order, orders_12m, net_sales, disputes in population():
        disputes = int(disputes or 0)
        parts = {
            "recency": 1.0 if last_order is None
            else (day - last_order).days / 365.0,
            "frequency": 1.0 - (orders_12m or 0) / 24.0,
            "value": 1.0 - (net_sales or 0) / 5_000_000.0,
            "support_load": disputes / 3.0,
            "open_dispute": 1.0 if disputes > 0 else 0.0,
        }
        out[customer] = sum(EXPECTED_WEIGHTS[name] * bounded(value)
                            for name, value in parts.items())
    return out


@lru_cache(maxsize=None)
def unbounded(day: dt.date) -> dict[str, float]:
    """The same week with the recency part left outside its unit — the answer
    the shipped module gives. Used only to prove a graded week can tell the two
    apart."""
    out: dict[str, float] = {}
    for customer, last_order, orders_12m, net_sales, disputes in population():
        disputes = int(disputes or 0)
        recency = 1.0 if last_order is None else min(1.0, (day - last_order).days / 365.0)
        out[customer] = (
            EXPECTED_WEIGHTS["recency"] * recency
            + EXPECTED_WEIGHTS["frequency"] * bounded(1.0 - (orders_12m or 0) / 24.0)
            + EXPECTED_WEIGHTS["value"] * bounded(1.0 - (net_sales or 0) / 5_000_000.0)
            + EXPECTED_WEIGHTS["support_load"] * bounded(disputes / 3.0)
            + EXPECTED_WEIGHTS["open_dispute"] * (1.0 if disputes > 0 else 0.0)
        )
    return out


# --------------------------------------------------------------------------
# The job under test
# --------------------------------------------------------------------------

def run_job(day: dt.date) -> None:
    """Score one week, the way the DAG's `features` and `score` tasks do."""
    from projects.customer.lib import churn

    churn.build_features(day.isoformat())
    churn.build_scores(day.isoformat())


#: What each graded week cost the first time it was scored: nothing, or the
#: fault it raised. A week that dies dies the same way every time, and scoring
#: 4,000 accounts again to watch it happen is a minute nobody gets back.
_SCORED: dict[dt.date, BaseException | None] = {}


def scored(day: dt.date) -> None:
    """Score a week once per session. Every test below reads what this left."""
    if day not in _SCORED:
        try:
            run_job(day)
            _SCORED[day] = None
        except BaseException as fault:  # noqa: BLE001 - re-raised below
            _SCORED[day] = fault
    fault = _SCORED[day]
    if fault is not None:
        raise fault


def rows_for(day: dt.date) -> list[tuple]:
    return query(
        f"SELECT {', '.join(JOB_COLUMNS)} FROM {JOB_TABLE} "
        "WHERE ds = ? ORDER BY customer_id",
        [day],
    )


def scores_for(day: dt.date) -> dict[str, float]:
    scored(day)
    rows = rows_for(day)
    assert rows, (
        f"FAIL: {day}: {JOB_TABLE_NAME} holds no row for that week after the "
        "job ran"
    )
    keys = [row[0] for row in rows]
    assert len(keys) == len(set(keys)), (
        f"FAIL: {day}: an account is scored more than once"
    )
    return {row[0]: float(row[2]) for row in rows}


# --------------------------------------------------------------------------
# The guards on the fixture itself
# --------------------------------------------------------------------------

def test_the_model_is_the_one_the_module_has_always_stated():
    """The weights are not this ticket's to move, and the expected scores below
    are built from them."""
    from projects.customer.lib import churn

    assert dict(churn.WEIGHTS) == EXPECTED_WEIGHTS, (
        f"FAIL: WEIGHTS is now {dict(churn.WEIGHTS)}; the ticket does not move "
        "the model"
    )


def test_the_graded_weeks_can_tell_the_answers_apart():
    """Each graded week must hold accounts whose last order falls after it, or
    the bounded answer and the shipped one agree and the week proves nothing."""
    for day in GRADED:
        gap = max(abs(expected(day)[key] - unbounded(day)[key])
                  for key in expected(day))
        assert gap > SCORE_TOLERANCE * 10, (
            f"FAIL: {day}: every account scores the same either way, so this "
            "week cannot grade the fix"
        )


# --------------------------------------------------------------------------
# The substance
# --------------------------------------------------------------------------

def test_a_week_inside_the_window_scores_every_account_correctly():
    check_week(IN_WINDOW)


def test_a_week_before_the_window_scores_every_account_correctly():
    check_week(BEFORE)


def test_a_week_the_ticket_never_mentions_scores_every_account_correctly():
    check_week(EARLIER)


def check_week(day: dt.date) -> None:
    want = expected(day)
    got = scores_for(day)
    missing = sorted(set(want) - set(got))
    extra = sorted(set(got) - set(want))
    assert not missing, (
        f"FAIL: {day}: {len(missing)} account(s) got no score, first "
        f"{missing[:3]} — every account the feature step holds gets a row"
    )
    assert not extra, f"FAIL: {day}: {len(extra)} account(s) scored that the 360 has not got, first {extra[:3]}"
    wrong = [(key, got[key], want[key]) for key in want
             if abs(got[key] - want[key]) > SCORE_TOLERANCE]
    assert not wrong, (
        f"FAIL: {day}: {len(wrong)} of {len(want)} score(s) are not the "
        f"weighted sum of the five parts, first {wrong[:3]} (got, wanted)"
    )


def test_every_score_lands_inside_the_scale_the_guard_states():
    """The check the rebuild died on. Every score on every graded week is
    inside nought-to-one, and the job's own guard says so too."""
    from projects.customer.lib import churn

    for day in GRADED:
        outside = [(key, value) for key, value in scores_for(day).items()
                   if value < 0 or value > 1]
        assert not outside, (
            f"FAIL: {day}: {len(outside)} score(s) sit outside nought-to-one, "
            f"first {outside[:3]}"
        )
        problems = churn.score_problems(day.isoformat())
        assert problems == [], (
            f"FAIL: {day}: the job's own check still objects to "
            f"{len(problems)} row(s), first {problems[:2]}"
        )


def test_the_nightly_model_s_table_is_left_alone():
    """One name, one writer. `marts.churn_scores_weekly` is the nightly model's
    and the job may not write a row into it."""
    for day in GRADED:
        scored(day)
    left = query(f"SELECT count(*) FROM {MODEL_TABLE}")[0][0]
    assert left == 0, (
        f"FAIL: the job put {left} row(s) into marts.churn_scores_weekly, "
        "which is the nightly model's table"
    )
    columns = [row[0] for row in query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'marts' AND table_name = 'churn_scores_weekly' "
        "ORDER BY ordinal_position")]
    assert "week_of" in columns and "ds" not in columns, (
        f"FAIL: marts.churn_scores_weekly now has columns {columns}; the "
        "nightly model's table was re-cut instead of left alone"
    )


def test_the_scale_is_on_every_row():
    """Two churn scores share a warehouse and neither name says which is which,
    so the row says it."""
    for day in GRADED:
        scored(day)
        scales = {str(row[3] or "").strip() for row in rows_for(day)}
        assert scales == {SCALE}, (
            f"FAIL: {day}: score_scale holds {sorted(scales)} and every row has "
            f"to carry the literal {SCALE!r}"
        )


def test_the_guard_still_objects_to_a_score_outside_the_scale():
    """A rebuild that goes green because the range was widened changed the
    label rather than the number. The row below is written to a week nothing
    grades and removed again."""
    from projects.customer.lib import churn

    scored(IN_WINDOW)
    statement(
        f"INSERT INTO {JOB_TABLE} ({', '.join(JOB_COLUMNS)}) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ["CHECK-ME", "copperline", -0.5, SCALE, 0, SCRATCH],
    )
    try:
        problems = churn.score_problems(SCRATCH.isoformat())
    finally:
        statement(f"DELETE FROM {JOB_TABLE} WHERE ds = ?", [SCRATCH])
    assert problems, (
        "FAIL: a score of -0.5 raised no objection, so the guard no longer "
        "states what a score on this table may be"
    )


def test_rebuilding_a_week_leaves_one_copy_of_it():
    """A repair that has to be run twice must not double the week."""
    scored(BEFORE)
    once = sorted(rows_for(BEFORE))
    run_job(BEFORE)
    assert sorted(rows_for(BEFORE)) == once, (
        f"FAIL: {BEFORE}: scoring the week again changed it"
    )


def test_the_published_file_keeps_its_name_and_carries_the_scale():
    """Retention's link points at `include/data/marts/churn_scores_<ds>.csv`."""
    from projects.customer.lib import churn

    scored(IN_WINDOW)
    written = churn.publish(IN_WINDOW.isoformat())
    wanted = warehouse.partition_path("marts", "churn_scores", IN_WINDOW)
    assert str(written) == str(wanted), (
        f"FAIL: the week was published to {written}, and retention reads "
        f"{wanted}"
    )
    with open(wanted, newline="", encoding="utf-8") as handle:
        published = list(csv.DictReader(handle))
    header = set(published[0]) if published else set()
    for column in ("customer_id", "churn_score", "score_scale"):
        assert column in header, (
            f"FAIL: the published file has no {column} column; it carries "
            f"{sorted(header)}"
        )
    assert len(published) == len(expected(IN_WINDOW)), (
        f"FAIL: the file holds {len(published)} row(s) and the week scored "
        f"{len(expected(IN_WINDOW))}"
    )
    assert {str(row["score_scale"] or "").strip() for row in published} == {SCALE}, (
        "FAIL: the published file does not say which scale it is on"
    )
    order = [float(row["churn_score"]) for row in published]
    assert order == sorted(order, reverse=True), (
        "FAIL: the published file is not highest score first"
    )
