"""RR-118 — is the restored record the record the landing copy actually holds?

Never ships to the agent. It runs beside the scored tree, after the graded run
of `fct_payments_restore`, with the tree root on `PYTHONPATH` — so it resolves
the warehouse and the landing tree the same way every DAG in the world does.

**No authored numbers.** Every expected result is computed from
`landing/meridian/` in the same session. The restore is graded against the
files it was told to rebuild from, never against a figure typed here, so the
task grades the same at either generator profile.

**What the numbers separate.** At the full profile the landing tree holds
75,067 events settled inside the requested window. The readings that lose are
not close:

    the whole tree, unfiltered                        486,282
    `raw.pay_meridian_settlements` over the window    318,039
    the tree filtered on the `dt` directory as well    71,967
    one directory per settlement date, `dt` = the
    settlement date (the shape the restore shipped)     1,697

Only a read that treats `dt` as an arrival date and filters on
`settlement_date` lands on the first figure.
"""

from __future__ import annotations

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves both trees from their own location rather than from the
# working directory.
from include.lib import landing_dir, warehouse

#: The settlement window RR-118 asks for. The ticket states both ends.
START = "2026-02-01"
END = "2026-03-31"

#: The columns the ticket lists, in the order it lists them.
COLUMNS = (
    "event_id", "payment_id", "intent_id", "order_ref", "processor_txn_id",
    "event_type", "amount_cents", "currency_code", "event_time_utc",
    "loaded_at", "settlement_date", "restates_event_id", "attempt_no",
    "deleted_at",
)

TABLE = "ops.payments_restored"

#: Every hourly file in the live Meridian landing tree. The whole tree, because
#: a settlement date is not a directory: the directories are arrival dates.
SOURCE = str(landing_dir("meridian") / "dt=*" / "hr=*" / "events.jsonl")

LANDED = (
    "read_json(?, format = 'newline_delimited', union_by_name = true)"
)


@pytest.fixture(scope="module")
def con():
    """One read-only connection to the warehouse the run wrote."""
    connection = duckdb.connect(str(warehouse.warehouse_path()), read_only=True)
    yield connection
    connection.close()


@pytest.fixture(scope="module")
def restored(con):
    """The restored table exists at all. Everything else depends on it."""
    schema, _, bare = TABLE.partition(".")
    found = con.execute(
        "select count(*) from information_schema.tables "
        "where table_schema = ? and table_name = ?", [schema, bare],
    ).fetchone()[0]
    assert found, f"{TABLE} was not built"
    return warehouse.qualify(TABLE)


def landed_scalar(con, select: str):
    """One aggregate over the landing files, inside the settlement window."""
    return con.execute(
        f"SELECT {select} FROM {LANDED} "
        "WHERE settlement_date::DATE BETWEEN ?::DATE AND ?::DATE",
        [SOURCE, START, END],
    ).fetchone()


def test_the_table_carries_the_columns_the_request_names(con, restored):
    """The fourteen the ticket lists, and no others.

    A `select *` over the landing tree carries DuckDB's hive columns `dt` and
    `hr` with it, which are our directory names and not Meridian's record.
    """
    schema, _, bare = TABLE.partition(".")
    got = [
        row[0] for row in con.execute(
            "select column_name from information_schema.columns "
            "where table_schema = ? and table_name = ? order by column_name",
            [schema, bare],
        ).fetchall()
    ]
    assert got == sorted(COLUMNS)


def test_the_restore_holds_the_events_the_landing_copy_holds(con, restored):
    """Count, distinct events and money, against the files themselves.

    The count is the whole discrimination: a restore that read the directory
    dates, or that read our own tables instead of the files, lands nowhere
    near it. The distinct count catches a window read twice.
    """
    want = landed_scalar(con, "count(*), count(DISTINCT event_id), sum(amount_cents)")
    got = con.execute(
        f"SELECT count(*), count(DISTINCT event_id), sum(amount_cents::BIGINT) "
        f"FROM {restored}"
    ).fetchone()
    assert want[0] > 0, "the landing tree holds nothing for the window"
    assert got == want


def test_every_restored_event_is_one_the_landing_copy_holds(con, restored):
    """The event ids, both ways.

    An id in the restore that the files do not hold is a row from somewhere
    else — our own tables, a mart, or a gap filled in from a neighbour. An id
    in the files that the restore does not hold is a date served short.
    """
    only_restored, only_landed = con.execute(
        f"""
        WITH landed AS (
            SELECT event_id FROM {LANDED}
            WHERE settlement_date::DATE BETWEEN ?::DATE AND ?::DATE
        ), restored AS (SELECT event_id FROM {restored})
        SELECT (SELECT count(*) FROM (SELECT event_id FROM restored
                                      EXCEPT SELECT event_id FROM landed)),
               (SELECT count(*) FROM (SELECT event_id FROM landed
                                      EXCEPT SELECT event_id FROM restored))
        """,
        [SOURCE, START, END],
    ).fetchone()
    assert only_restored == 0, f"{only_restored} restored events are in no landing file"
    assert only_landed == 0, f"{only_landed} events in the landing files were not restored"


def test_nothing_outside_the_requested_window_was_restored(con, restored):
    """The request is a settlement window and the table is the answer to it.

    A restore that wrote the whole tree, or that wrote by arrival date, puts
    settlement dates outside the window into a record legal will send back to
    Meridian.
    """
    outside = con.execute(
        f"SELECT count(*) FROM {restored} "
        "WHERE settlement_date::DATE < ?::DATE OR settlement_date::DATE > ?::DATE",
        [START, END],
    ).fetchone()[0]
    assert outside == 0


def test_each_settlement_date_holds_what_the_files_hold_for_it(con, restored):
    """Date by date, not only in total.

    Two errors that cancel in a total do not cancel here, and this is also
    where a date filled in from a neighbour shows up: the six days whose files
    are gone keep exactly the events that arrived on other days, and no more.
    """
    breaks = con.execute(
        f"""
        WITH landed AS (
            SELECT settlement_date::DATE AS settlement_date, count(*) AS events
            FROM {LANDED}
            WHERE settlement_date::DATE BETWEEN ?::DATE AND ?::DATE
            GROUP BY 1
        ), restored AS (
            SELECT settlement_date::DATE AS settlement_date, count(*) AS events
            FROM {restored} GROUP BY 1
        )
        SELECT settlement_date,
               coalesce(landed.events, 0), coalesce(restored.events, 0)
        FROM landed FULL OUTER JOIN restored USING (settlement_date)
        WHERE coalesce(landed.events, 0) <> coalesce(restored.events, 0)
        ORDER BY settlement_date
        """,
        [SOURCE, START, END],
    ).fetchall()
    assert not breaks, f"{len(breaks)} settlement date(s) disagree: {breaks[:5]}"
