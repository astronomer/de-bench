"""MER-618 — does the patched product join fill the enriched spine's three
product columns, and does it still run for the close?

Never ships to the agent. It runs beside the scored tree with the tree root on
`PYTHONPATH`, so it imports the patched `orders_enrich_daily` the way the dag
processor does and calls `join_product` with the date, which is the only
argument the graph gives it.

**Why a verifier rather than a DAG run.** `AGENTS.md`, "The warehouse": the
world ships the landed half only, so `staging.*` is not on disk, and
`staging.orders_enriched` has no DDL anywhere in the tree — `publish_enriched`
stops on it whatever the agent writes above it. The ticket says so and says to
drive `build_base` and `join_product` by hand. This file does the same thing.

**No authored numbers.** Every expected result is computed from `raw.orders`,
`raw.order_lines` and `raw.pim_product_versions` in the same session that runs
the step, so an edit to the tree cannot move the answer.

**Three order days.**

    2025-10-08  FY2025, before the UTC cutover
    2026-04-15  FY2026, and the one day of the three that carries a change
                stamped during the order day itself
    2026-05-20  eight weeks later, and the day with the most retired SKUs on it

None of the three is named in the ticket, none is an era boundary, and none is a
closure day. The guard tests below say what each one is able to separate before
anything about the answer is graded.

**The close runs the same statement.** `nightly_close.build_step` binds the
date and nothing else, so a repair that gives the file a second placeholder
fixes this DAG and breaks the close. The last test drives the close's own
`join_product` for one of the graded days.
"""

from __future__ import annotations

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the way every DAG in the world does.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.orders` under the same name.
ORDERS = warehouse.qualify("raw.orders")
LINES = warehouse.qualify("raw.order_lines")
PIM = warehouse.qualify("raw.pim_product_versions")
STAGING = warehouse.qualify("staging")
ENRICH_BASE = warehouse.qualify("staging.enrich_base")
ENRICH_PRODUCT = warehouse.qualify("staging.enrich_product")

#: The days the step is driven over. See the module docstring.
FIRST = "2025-10-08"
MIDDAY = "2026-04-15"
LATE = "2026-05-20"
GRADED = (FIRST, MIDDAY, LATE)

#: The tie-break `stg_product__pim_versions` and `snap_product_price` both
#: spell: a person's edit beats a supplier's file, and a bulk load is weakest.
SOURCE_RANK = """
    CASE source
        WHEN 'pim_ui' THEN 1
        WHEN 'supplier_feed' THEN 2
        WHEN 'bulk_load' THEN 3
        ELSE 4
    END
"""

#: What the enrichment leaves for the date: the day's orders, staff
#: transactions dropped, soft-deleted orders kept and flagged. Written here
#: rather than by running the tree's own `enrich_base.sql`, so that the
#: population this step is measured against is an input to this file and not
#: something the tree under test decides. `enrich_base.sql` is protected, so
#: the two agree.
ENRICH_BASE_SQL = f"""
CREATE OR REPLACE TABLE {ENRICH_BASE} AS
SELECT order_id, customer_ref, loyalty_id, brand, channel, store_id,
       market_code, local_order_date, event_time_utc, event_time_local,
       order_status, currency_code, fx_rate_ppm, subtotal_cents,
       order_discount_cents, tax_cents, shipping_cents, grand_total_cents,
       gift_card_applied_cents, order_ref, source_system,
       deleted_at IS NOT NULL AS deleted_at_source
FROM {ORDERS}
WHERE local_order_date = CAST(? AS DATE)
  AND NOT is_test
"""

#: The record in force on a date, off the feed: rank every change stamped on or
#: before that date, newest date first, source rank next, later stamp next,
#: `change_id` last; keep the winner; and keep it only if it is an upsert,
#: because a delete retires the SKU and opens nothing. This is the ticket's
#: contract and there is nothing else behind it.
IN_FORCE = f"""
ranked AS (
    SELECT sku, category_id, operation,
           row_number() OVER (
               PARTITION BY sku
               ORDER BY CAST(updated_at AS DATE) DESC,
                        {SOURCE_RANK},
                        updated_at DESC,
                        change_id
           ) AS version_seq
    FROM {PIM}
    WHERE CAST(updated_at AS DATE) <= CAST(? AS DATE)
),
in_force AS (
    SELECT sku, category_id
    FROM ranked
    WHERE version_seq = 1
      AND operation = 'upsert'
)
"""

#: The day the step owes, out of the order book and the feed.
EXPECTED_DAY = f"""
WITH base AS (
    SELECT order_id FROM {ORDERS}
     WHERE local_order_date = CAST(? AS DATE) AND NOT is_test
),
{IN_FORCE}
SELECT b.order_id,
       CAST(count(*) AS BIGINT),
       CAST(sum(l.qty) AS DOUBLE),
       CAST(count(DISTINCT p.category_id) AS BIGINT)
FROM base b
JOIN {LINES} l ON l.order_id = b.order_id
LEFT JOIN in_force p ON p.sku = l.sku
GROUP BY b.order_id
ORDER BY b.order_id
"""

#: The same day read with today's record instead of the record in force. Kept
#: to prove a graded day can tell an as-of read from a current-row one.
TODAYS_RECORD_DAY = f"""
WITH base AS (
    SELECT order_id FROM {ORDERS}
     WHERE local_order_date = CAST(? AS DATE) AND NOT is_test
),
ranked AS (
    SELECT sku, category_id, operation,
           row_number() OVER (
               PARTITION BY sku
               ORDER BY CAST(updated_at AS DATE) DESC,
                        {SOURCE_RANK},
                        updated_at DESC,
                        change_id
           ) AS version_seq
    FROM {PIM}
),
in_force AS (
    SELECT sku, category_id FROM ranked
     WHERE version_seq = 1 AND operation = 'upsert'
)
SELECT b.order_id,
       CAST(count(DISTINCT p.category_id) AS BIGINT)
FROM base b
JOIN {LINES} l ON l.order_id = b.order_id
LEFT JOIN in_force p ON p.sku = l.sku
GROUP BY b.order_id
ORDER BY b.order_id
"""

#: How many of the day's SKUs were retired by the order date — the population
#: that vanishes if the deletes are filtered out before the ranking.
RETIRED_ON_DAY = f"""
WITH sold AS (
    SELECT DISTINCT l.sku
    FROM {ORDERS} o
    JOIN {LINES} l ON l.order_id = o.order_id
    WHERE o.local_order_date = CAST(? AS DATE) AND NOT o.is_test
),
ranked AS (
    SELECT sku, operation,
           row_number() OVER (
               PARTITION BY sku
               ORDER BY CAST(updated_at AS DATE) DESC,
                        {SOURCE_RANK},
                        updated_at DESC,
                        change_id
           ) AS version_seq
    FROM {PIM}
    WHERE CAST(updated_at AS DATE) <= CAST(? AS DATE)
)
SELECT count(*) FROM ranked r
JOIN sold s ON s.sku = r.sku
WHERE r.version_seq = 1 AND r.operation = 'delete'
"""

#: Orders on the day with no catalogued line at all — the ones an inner join
#: loses out of the enrichment, silently.
UNCATALOGUED_ORDERS = f"""
WITH base AS (
    SELECT order_id FROM {ORDERS}
     WHERE local_order_date = CAST(? AS DATE) AND NOT is_test
),
{IN_FORCE}
SELECT count(*) FROM (
    SELECT b.order_id
    FROM base b
    JOIN {LINES} l ON l.order_id = b.order_id
    LEFT JOIN in_force p ON p.sku = l.sku
    GROUP BY b.order_id
    HAVING count(p.sku) = 0
)
"""

ACTUAL_DAY = f"""
SELECT order_id,
       CAST(line_count AS BIGINT),
       CAST(unit_count AS DOUBLE),
       CAST(category_count AS BIGINT)
FROM {ENRICH_PRODUCT}
ORDER BY order_id
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One statement on its own connection. DuckDB takes one writer and the
    step under test opens its own, so nothing here may hold the file."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def one(sql: str, params: list | None = None):
    return query(sql, params)[0][0]


def enrich_module():
    """The patched DAG module, imported the way the dag processor imports it."""
    from projects.commerce.dags import orders_enrich_daily

    return orders_enrich_daily


def close_module():
    from projects.commerce.dags import nightly_close

    return nightly_close


def call(module, task_id: str, ds: str):
    """One step, through its own callable and its own kwargs, with the date
    filled in wherever the graph templates it."""
    task = module.dag.task_dict[task_id]
    kwargs = {
        key: (ds if isinstance(value, str) and "{{" in value else value)
        for key, value in (task.op_kwargs or {}).items()
    }
    return task.python_callable(**kwargs)


def build(ds: str) -> None:
    """The base and the product join for one date, as the graph would run
    them. The base is laid by this file; the join is the patched step."""
    query(ENRICH_BASE_SQL, [ds])
    call(enrich_module(), "join_product", ds)


def actual() -> list[tuple]:
    return query(ACTUAL_DAY)


def expected(ds: str) -> list[tuple]:
    return query(EXPECTED_DAY, [ds, ds])


@pytest.fixture(scope="session")
def joined():
    """Drive the step over the three days, then over the first day again, and
    keep what each pass left behind. One pass per day and no more: every read
    below comes out of this dictionary."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING}")
    finally:
        con.close()

    out: dict = {"rows": {}}
    for ds in GRADED:
        build(ds)
        out["rows"][ds] = actual()

    # The same date a second time. `build` lays the base again first, which is
    # what a rerun of the DAG does.
    build(FIRST)
    out["rerun"] = actual()
    return out


def test_the_graded_days_can_tell_an_as_of_read_from_todays_record():
    """The fixture's own guard. On each graded day the record in force has to
    differ from the record the SKU is on today for at least one order, or the
    day cannot separate the two readings."""
    for ds in GRADED:
        asof = {row[0]: row[3] for row in expected(ds)}
        today = dict(query(TODAYS_RECORD_DAY, [ds]))
        moved = sum(1 for key in asof if asof[key] != today.get(key))
        assert moved, (
            f"FAIL: {ds}: today's record and the record in force agree on every "
            "order, so this day cannot grade the as-of rule"
        )


def test_the_graded_days_carry_a_sku_the_catalogue_had_retired():
    """The second guard. A day with no retired SKU on it cannot tell a build
    that ranks the deletes from one that drops them first."""
    retired = {ds: one(RETIRED_ON_DAY, [ds, ds]) for ds in GRADED}
    assert all(retired.values()), (
        f"FAIL: no retired SKU sold on {sorted(k for k, v in retired.items() if not v)}, "
        "so those days cannot grade the retirement rule"
    )


def test_the_graded_days_carry_orders_with_no_catalogued_line():
    """The third guard. The coverage rule is only graded on a day that holds
    orders the catalogue cannot speak for at all."""
    for ds in GRADED:
        assert one(UNCATALOGUED_ORDERS, [ds, ds]), (
            f"FAIL: {ds}: every order has a catalogued line, so this day cannot "
            "grade the coverage rule"
        )


@pytest.mark.parametrize("ds", GRADED)
def test_every_order_in_the_base_gets_one_row(joined, ds):
    """The coverage rule and the grain together. One row per order in
    `staging.enrich_base` for the date, and no others."""
    got = [row[0] for row in joined["rows"][ds]]
    want = [row[0] for row in expected(ds)]
    assert len(got) == len(set(got)), (
        f"FAIL: {ds}: {len(got) - len(set(got))} orders came out more than once"
    )
    missing = sorted(set(want) - set(got))
    extra = sorted(set(got) - set(want))
    assert not missing, (
        f"FAIL: {ds}: {len(missing)} orders in the base have no row, the first "
        f"of them {missing[0]}; the order book is wider than the catalogue and "
        "every order keeps its row"
    )
    assert not extra, (
        f"FAIL: {ds}: {len(extra)} orders came out that are not in the base, the "
        f"first of them {extra[0]}"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_the_line_and_unit_counts_tie_to_the_order_book(joined, ds):
    """`line_count` is the order's lines and `unit_count` is their quantity.
    A product join that fans out inflates both."""
    got = {row[0]: (row[1], row[2]) for row in joined["rows"][ds]}
    want = {row[0]: (row[1], row[2]) for row in expected(ds)}
    wrong = sorted(key for key in want if got.get(key) != want[key])
    assert not wrong, (
        f"FAIL: {ds}: {len(wrong)} orders carry the wrong line or unit count; "
        f"{wrong[0]} came out {got.get(wrong[0])} against {want[wrong[0]]} in "
        "the order book"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_the_categories_are_the_ones_in_force_on_the_order_date(joined, ds):
    """`category_count` under the record in force on the order's business
    date: the newest change stamped on or before it, the source tie-break
    where a date carries two, and nothing at all for a SKU a delete retired."""
    got = {row[0]: row[3] for row in joined["rows"][ds]}
    want = {row[0]: row[3] for row in expected(ds)}
    wrong = sorted(key for key in want if got.get(key) != want[key])
    assert not wrong, (
        f"FAIL: {ds}: {len(wrong)} orders carry the wrong category count; "
        f"{wrong[0]} came out {got.get(wrong[0])} against the {want[wrong[0]]} "
        f"in force on {ds}"
    )


def test_running_the_step_twice_leaves_the_same_table(joined):
    """A rerun of the date leaves the table as the first run left it."""
    assert joined["rerun"] == joined["rows"][FIRST], (
        f"FAIL: {FIRST}: the second run changed the day"
    )


def test_the_close_still_runs_the_same_statement(joined):
    """`nightly_close` runs this file too, through `build_step`, which binds
    the date and nothing else. A statement given a second placeholder repairs
    this DAG and breaks the close."""
    query(ENRICH_BASE_SQL, [LATE])
    try:
        call(close_module(), "join_product", LATE)
    except Exception as exc:  # noqa: BLE001 — whether it raises is the point
        pytest.fail(
            f"FAIL: {LATE}: nightly_close's join_product no longer runs the "
            f"statement: {exc!r}"
        )
    assert actual() == joined["rows"][LATE], (
        f"FAIL: {LATE}: the close's join_product left a different table from "
        "the enrichment's, on the same date"
    )
