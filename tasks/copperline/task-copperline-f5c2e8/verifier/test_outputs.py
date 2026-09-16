"""CUS-476 — does the guest bucket still hold the denominator, and has it
stopped answering questions about a life?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it resolves the warehouse the way every DAG in the world does.

**No authored numbers.** Every expected result is computed from `raw.orders` in
the same session, under the two filters `stg_sales__orders` applies. Nothing
here holds a row count, a market code or a date typed by hand, so a rebuild of
the world cannot make this verifier stale.

**Why dbt runs here.** The world ships the landed half of the warehouse only
(`AGENTS.md`, "The warehouse"), so `int_customer_lifecycle` does not exist until
dbt has made it. The `prepare` step in checks.yaml builds it and its ancestors —
staging views and two shared intermediates. This file runs `dbt test` twice more
and nothing here may hold the warehouse open while it does: DuckDB takes one
writer.

**The seven columns.** The ticket names them, so they are the contract:
`first_order_date`, `last_order_date`, `tenure_days`, `days_since_last_order`,
`is_churned`, `is_one_and_done`, `is_new`. Empty on a bucket, present on
everything else. The columns the ticket says stay are checked too, because the
cheapest way to make the first list empty is to throw the row away.
"""

from __future__ import annotations

import os
import subprocess

import duckdb
import pytest

from include.lib import warehouse, workspace_root

DB = str(warehouse.warehouse_path())

ORDERS = warehouse.qualify("raw.orders")
MODEL = warehouse.qualify('"int".int_customer_lifecycle')

#: The two filters `stg_sales__orders` applies and every reader of the spine
#: inherits. Written here rather than read from the staging view so the oracle
#: does not depend on the tree it is grading.
SPINE = "not coalesce(is_test, false) and deleted_at is null"

#: The columns that describe one customer's history. Empty on a guest row.
LIFE_COLUMNS = (
    "first_order_date",
    "last_order_date",
    "tenure_days",
    "days_since_last_order",
    "is_churned",
    "is_one_and_done",
    "is_new",
)

#: The columns the ticket says stay on a guest row: the counts and the money the
#: bucket exists to hold.
DENOMINATOR_COLUMNS = (
    "orders_lifetime",
    "orders_12m",
    "channels_used",
    "booked_cents_lifetime",
    "net_sales_cents_lifetime",
    "average_order_net_cents",
)

#: The pinned dbt. `dbt/README.md` says a bare `dbt` is either not on the PATH
#: or is not the pinned one. The override exists so this file can be exercised
#: against a tree on a laptop; nothing in a trial or in scoring sets it.
DBT = os.environ.get("DE_BENCH_DBT", "/opt/dbt-venv/bin/dbt")
PROJECT = str(workspace_root() / "dbt" / "copperline_analytics")


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. Nothing holds the file open: dbt wants
    the single writer and this runs between its invocations."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def one(sql: str, params: list | None = None):
    rows = query(sql, params)
    return rows[0][0] if rows else None


def dbt(*args: str) -> subprocess.CompletedProcess:
    """Run the pinned dbt in the project directory, profile alongside."""
    return subprocess.run(
        [DBT, *args, "--profiles-dir", "."],
        cwd=PROJECT, capture_output=True, text=True, timeout=900, check=False,
    )


def model_built() -> bool:
    try:
        query(f"SELECT 1 FROM {MODEL} LIMIT 1")
    except duckdb.Error:
        return False
    return True


@pytest.fixture(scope="session", autouse=True)
def built():
    """The model has to exist before anything is graded. The prepare step builds
    it; if that failed, the model does not compile and that is the finding."""
    if not model_built():
        result = dbt("run", "--select", "+int_customer_lifecycle")
        assert model_built(), (
            "FAIL: int_customer_lifecycle does not build, so nothing about it "
            f"can be read:\n{(result.stdout or '')[-2000:]}"
        )


def guest_orders_by_market() -> dict[str, int]:
    """The bucket the model is meant to publish, computed from the feed: one
    key per market that took an order naming nobody, and its orders."""
    rows = query(f"""
        SELECT 'GUEST-' || market_code, count(*)
        FROM {ORDERS}
        WHERE {SPINE} AND customer_ref IS NULL AND loyalty_id IS NULL
        GROUP BY 1
    """)
    return {key: int(n) for key, n in rows}


# ---------------------------------------------------------------- the oracle

def test_the_feed_still_carries_orders_that_name_nobody():
    """The oracle checks itself against the world before it grades anything.

    Everything below rests on there being an account-less population, spread
    over more than one market, and large enough that collapsing it onto one row
    per market is the thing the ticket says it is. If a rebuild of the world
    moves any of that, this says so rather than letting the tests below grade
    against a feed that has changed shape.
    """
    spine = int(one(f"SELECT count(*) FROM {ORDERS} WHERE {SPINE}"))
    buckets = guest_orders_by_market()
    guest = sum(buckets.values())

    assert spine > 0, "FAIL: the order spine is empty"
    assert len(buckets) > 1, (
        f"FAIL: orders naming nobody reach {len(buckets)} market(s); the bucket "
        "grain the ticket is about needs more than one"
    )
    share = guest / spine
    assert 0.05 < share < 0.30, (
        f"FAIL: {guest} of {spine} spine orders name nobody ({share:.1%}); the "
        "world has moved off the population this ticket was written against"
    )


# ------------------------------------------------- the bucket is still a bucket

def test_the_buckets_are_still_one_row_per_market_and_still_hold_their_orders():
    """One row per market, none dropped, none split, every order still on it.

    This is the check that convicts the two cheap ways of emptying the life
    columns: throwing the guest rows away, and giving every guest order an
    identity of its own so that the bucket grain disappears. Both leave the
    model without the denominator it exists to publish.
    """
    expected = guest_orders_by_market()
    rows = query(f"""
        SELECT party_key, orders_lifetime, is_guest
        FROM {MODEL} WHERE party_kind = 'guest'
    """)
    actual = {key: int(n) for key, n, _ in rows}

    assert len(rows) == len(actual), (
        f"FAIL: {len(rows)} guest rows carry {len(actual)} distinct keys; the "
        "bucket grain is one row per market"
    )
    assert set(actual) == set(expected), (
        "FAIL: the guest rows are not one per market. "
        f"missing {sorted(set(expected) - set(actual))[:6]}, "
        f"unexpected {sorted(set(actual) - set(expected))[:6]}"
    )
    wrong = {k: (actual[k], expected[k]) for k in expected if actual[k] != expected[k]}
    assert not wrong, (
        "FAIL: a bucket lost or gained orders — got vs expected "
        f"{dict(list(wrong.items())[:6])}"
    )
    assert all(flag for _, _, flag in rows), (
        "FAIL: a guest row does not carry is_guest"
    )


def test_the_denominator_is_still_on_the_bucket():
    """The counts and the money stay. `average_order_net_cents` is an average
    over orders rather than a claim about a life, and the ticket says it stays
    with them, so a blanket NULL over the guest row is convicted here.
    """
    missing = []
    for column in DENOMINATOR_COLUMNS:
        try:
            nulls = int(one(
                f"SELECT count(*) FROM {MODEL} "
                f"WHERE party_kind = 'guest' AND {column} IS NULL"
            ))
        except duckdb.Error as exc:
            missing.append(f"{column} is gone from the model ({exc})")
            continue
        if nulls:
            missing.append(f"{column} on {nulls} bucket row(s)")
    assert not missing, (
        "FAIL: the bucket lost part of the denominator it exists to hold: "
        + "; ".join(missing)
    )


# ------------------------------------------------- the bucket claims no life

def test_a_bucket_no_longer_claims_a_life():
    """The seven columns the ticket names, empty on every guest row.

    Empty and not false: a guest row reading `is_churned = false` is the claim
    the ticket is about, so a fix that writes false, zero or a sentinel date
    fails here the same way no fix at all does.
    """
    still_claiming = []
    for column in LIFE_COLUMNS:
        try:
            rows = int(one(
                f"SELECT count(*) FROM {MODEL} "
                f"WHERE party_kind = 'guest' AND {column} IS NOT NULL"
            ))
        except duckdb.Error as exc:
            still_claiming.append(f"{column} is gone from the model ({exc})")
            continue
        if rows:
            sample = one(
                f"SELECT {column} FROM {MODEL} "
                f"WHERE party_kind = 'guest' AND {column} IS NOT NULL LIMIT 1"
            )
            still_claiming.append(f"{column} on {rows} bucket row(s), e.g. {sample!r}")
    assert not still_claiming, (
        "FAIL: a bucket still answers a question about a life: "
        + "; ".join(still_claiming)
    )


def test_the_change_stopped_at_the_bucket():
    """Every party that is not a bucket still carries all seven.

    A trade or loyalty party always has at least one order, so all seven are
    answerable on it. This convicts the blanket fix — nulling the columns for
    everybody — and it convicts dropping the columns from the model, which is
    the other way to make the test above green.
    """
    parties = int(one(f"SELECT count(*) FROM {MODEL} WHERE party_kind <> 'guest'"))
    assert parties > 0, "FAIL: the model publishes no party that is not a bucket"

    emptied = []
    for column in LIFE_COLUMNS:
        try:
            nulls = int(one(
                f"SELECT count(*) FROM {MODEL} "
                f"WHERE party_kind <> 'guest' AND {column} IS NULL"
            ))
        except duckdb.Error as exc:
            emptied.append(f"{column} is gone from the model ({exc})")
            continue
        if nulls:
            emptied.append(f"{column} empty on {nulls} of {parties} real parties")
    assert not emptied, (
        "FAIL: the change reached past the bucket rows: " + "; ".join(emptied)
    )


# ------------------------------------------------------------ the model's tests

def test_the_model_still_passes_its_own_tests():
    """`dbt test --select int_customer_lifecycle`, clean.

    The `not_null` on `first_order_date` goes red on the nine buckets the moment
    they go empty. Leaving it as it was fails here.
    """
    result = dbt("test", "--select", "int_customer_lifecycle")
    blob = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, (
        "FAIL: the model's own tests do not pass after the change:\n"
        + blob[-2000:]
    )


def test_a_missing_first_order_date_on_a_real_party_still_goes_red():
    """The assertion still asserts something.

    The test above passes just as well when the `not_null` has been deleted,
    turned down to a warning, or scoped to nothing. So a first order date is
    taken off one party that is not a bucket, `dbt test` is run again, and it
    has to find it. The value is put back afterwards whatever happens.

    Nothing is rebuilt: the model materializes as a table, so the row is edited
    in place and `dbt test` reads the same relation dbt would.
    """
    victim = one(f"""
        SELECT party_key FROM {MODEL}
        WHERE party_kind <> 'guest' AND first_order_date IS NOT NULL
        ORDER BY party_key LIMIT 1
    """)
    assert victim, "FAIL: no party outside the buckets carries a first order date"
    kept = one(
        f"SELECT first_order_date FROM {MODEL} WHERE party_key = ?", [victim]
    )

    def write(value) -> None:
        con = duckdb.connect(DB)
        try:
            con.execute(
                f"UPDATE {MODEL} SET first_order_date = ? WHERE party_key = ?",
                [value, victim],
            )
        finally:
            con.close()

    write(None)
    try:
        result = dbt("test", "--select", "int_customer_lifecycle")
        blob = (result.stdout or "") + (result.stderr or "")
        assert result.returncode != 0, (
            f"FAIL: the first order date was taken off {victim}, which is not a "
            "bucket, and the model's tests still passed — the assertion has been "
            f"dropped, muted or scoped to nothing:\n{blob[-2000:]}"
        )
    finally:
        write(kept)
