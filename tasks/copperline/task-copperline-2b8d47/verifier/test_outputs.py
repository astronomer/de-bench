"""POS-402 — does the 04:00 escalation name the stores whose feed stopped?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched DAG module the way Airflow would and
calls the same function the `escalate_silent_stores` task calls.

**Why a verifier rather than a DAG run.** The escalation is one function over
one table. Running the whole catch-up would also run its two load steps, and
those rewrite manifest rows for any business date that still has files on disk
— the table every expected result here is computed from. Calling the task's own
callable grades the same code, the same statement and the same parameters, and
leaves the manifest as it landed.

**No authored answer.** Every expected result on a real night is computed from
`raw.pos_batch_manifest` in the same session. Even which status words mean a
batch arrived is read off the landed table rather than typed here: the row the
manifest keeps for a batch that never came carries no row count, so the words
with a count against them are the delivered ones.

**Four nights, and the ticket names none of them.**

    2026-01-15  an ordinary night. No store's feed has stopped, so the
                escalation names nobody.
    2026-02-19  a store whose batches stopped a fortnight earlier, and whose
                batches start again in March — so a query that reads the whole
                manifest instead of the window up to the run's date says this
                night was fine.
    2026-04-06  a market holiday. The only store whose feed has stopped sits in
                that market, so POS-1's calendar check is what decides the
                night.
    2026-05-07  eleven weeks later, and a different store.

**And one estate the world does not hold.** The last four tests build a small
warehouse of their own — eight stores in two markets, a manifest and a calendar
the fixtures never mention — and run the same code against it. A query that
names the right stores in Copperline because it was told their names fails
there, and so does one that reads the manifest with no upper bound on the
business date.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does.
from include.lib import warehouse

LIVE = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# include/lib/warehouse.py gives: the frozen `nwv` copy sits first on the
# search path and carries some of these names too.
MANIFEST = warehouse.qualify("raw.pos_batch_manifest")
STORES = warehouse.qualify("raw.stores")
CALENDAR = warehouse.qualify("raw.market_calendar")
ESCALATIONS = warehouse.qualify("ops.pos_escalations")

#: The four nights. See the module docstring. All four sit outside the world's
#: reserved windows and outside the never-grade tail, and the ticket names none
#: of them.
QUIET_NIGHT = "2026-01-15"
FIRST_DARK = "2026-02-19"
MARKET_HOLIDAY = "2026-04-06"
SECOND_DARK = "2026-05-07"
GRADED = (QUIET_NIGHT, FIRST_DARK, MARKET_HOLIDAY, SECOND_DARK)

#: The synthetic estate's own night, and how far its calendar reaches either
#: side of it. Nothing about these dates touches the world.
SYNTH_DS = dt.date(2025, 7, 16)
SYNTH_FIRST = SYNTH_DS - dt.timedelta(days=40)
SYNTH_LAST = SYNTH_DS + dt.timedelta(days=10)


# --- reading -----------------------------------------------------------------


def query(sql: str, params: list | None = None, db: str | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the code
    under test opens its own, so nothing here may hold a file open."""
    con = duckdb.connect(db or LIVE)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def statuses() -> tuple[set[str], set[str]]:
    """Every status the landed manifest uses, and the ones that mean the batch
    arrived.

    Derived, not authored. `_manifest` in the generator writes NULL counts and
    a NULL stamp for the store-day whose batch never came, so a status with a
    row count against it is a status a file was loaded under.
    """
    rows = query(
        f"SELECT status, count(*) FILTER (WHERE row_count IS NOT NULL) "
        f"FROM {MANIFEST} GROUP BY status"
    )
    return {row[0] for row in rows}, {row[0] for row in rows if row[1]}


def silent(ds: str, lookback: int) -> list[tuple]:
    """The stores POS-1 escalates on a date, straight from the landed manifest.

    The ticket's rule, and nothing else: a store that has delivered a batch
    before, whose last delivered business date on or before the run's date is
    further back than the lookback, in a market that was expecting a feed that
    night.
    """
    _, delivered = statuses()
    marks = ", ".join("?" for _ in delivered)
    return query(
        f"""
        WITH delivered AS (
            SELECT store_id, max(business_date) AS last_seen
            FROM {MANIFEST}
            WHERE status IN ({marks}) AND business_date <= ?::DATE
            GROUP BY store_id
        ),
        estate AS (SELECT DISTINCT store_id, market_code FROM {STORES})
        SELECT d.store_id, d.last_seen
        FROM delivered d
        JOIN estate s ON s.store_id = d.store_id
        LEFT JOIN {CALENDAR} c
               ON c.market_code = s.market_code AND c.calendar_date = ?::DATE
        WHERE coalesce(c.feed_expected, true)
          AND d.last_seen < ?::DATE - ?
        ORDER BY d.store_id
        """,
        [*sorted(delivered), ds, ds, ds, lookback],
    )


# --- the code under test -----------------------------------------------------


def catchup():
    """The patched catch-up module, imported the way the dag processor does."""
    from projects.commerce.dags import store_pos_late_catchup

    return store_pos_late_catchup


def lookback() -> int:
    """The window the job itself works to. Read from the module so a change to
    it moves the expected result with it."""
    return int(getattr(catchup(), "LOOKBACK_DAYS", 3))


def escalate(ds: str) -> None:
    """Run the escalation for one date, the way the DAG's last task does."""
    catchup().escalate_silent_stores(ds)


def escalated(ds: str, db: str | None = None) -> list[tuple]:
    """What the escalation wrote for a date."""
    try:
        return query(
            f"SELECT store_id, last_seen FROM {ESCALATIONS} "
            "WHERE ds = ?::DATE ORDER BY store_id",
            [ds],
            db=db,
        )
    except duckdb.CatalogException as exc:
        raise AssertionError(
            f"FAIL: {ds}: the run left no {ESCALATIONS} to read — {exc}"
        ) from None


def check_night(ds: str) -> None:
    expected = silent(ds, lookback())
    escalate(ds)
    assert escalated(ds) == expected, (
        f"FAIL: {ds}: the escalation named {escalated(ds)}, the manifest says "
        f"{expected}"
    )


# --- the synthetic estate ----------------------------------------------------


def days(first: dt.date, last: dt.date):
    day = first
    while day <= last:
        yield day
        day += dt.timedelta(days=1)


def manifest_row(store: str, day: dt.date, status: str) -> tuple:
    counted = None if status == "missing" else 40
    # `received_at` follows the landed table's own rule: a batch that arrived
    # carries the moment it did, and only a `missing` store-day has none. The
    # fixture used to leave it NULL on every row, so a query that bounds on
    # arrival time — which the ticket asks for ("what has arrived since is not
    # what that night knew") — saw no batch as ever having arrived.
    received = None if status == "missing" else dt.datetime.combine(
        day + dt.timedelta(days=1), dt.time(6, 0))
    return (
        f"B-{day:%Y%m%d}-{store}", store, day, f"store_{store}.csv",
        counted, counted, status, received,
    )


@pytest.fixture(scope="session")
def estate(tmp_path_factory) -> str:
    """A warehouse of six stores in two markets, none of which Copperline holds.

    The tables are made from the landed warehouse's own column lists, so any
    column the patched statement reads is here, and the manifest keeps its
    primary key so `INSERT OR REPLACE` behaves as it does in the world.

    Relative to the estate's night D, and with `raw.stores` holding all eight
    stores from the start:

        S-9001  a batch every night through D                  quiet
        S-9002  batches to D-9, then nothing but missing rows   silent, D-9
        S-9003  missing rows only, never a batch                not open yet
        S-9004  batches to D-20, nothing until D+1              silent, D-20
        S-9005  missing rows, then a late batch on D-1 and D    quiet
        S-9006  batches to D-25, in a market shut on D          not escalated
        S-9007  nothing at all until the load test lands one
        S-9008  batches to D-30, and the dimension calls it
                closed                                         silent, D-30
    """
    root = tmp_path_factory.mktemp("estate")
    path = root / "copperline.duckdb"
    columns = query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = 'pos_batch_manifest' "
        "ORDER BY ordinal_position"
    )
    con = duckdb.connect(str(path))
    try:
        con.execute(f"ATTACH '{LIVE}' AS landed (READ_ONLY)")
        con.execute("CREATE SCHEMA raw")
        for name in ("stores", "market_calendar", "pos_sales_header"):
            con.execute(
                f"CREATE TABLE raw.{name} AS "
                f"SELECT * FROM landed.raw.{name} WHERE false"
            )
        con.execute(
            "CREATE TABLE raw.pos_batch_manifest ("
            + ", ".join(f"{name} {kind}" for name, kind in columns)
            + ", PRIMARY KEY (batch_id))"
        )
        con.execute("DETACH landed")

        con.executemany(
            # `valid_from`/`valid_to` follow the landed table's own shape: it is
            # SCD2 (290 rows for 268 stores) and every live row carries a
            # valid_from. Leaving them NULL made the standard as-of filter — the
            # idiom three sibling files in projects/commerce/sql/ teach — match
            # no row, so a correct calendar check silently became a no-op.
            "INSERT INTO raw.stores (store_id, market_code, status, is_current, "
            "valid_from, valid_to) "
            "VALUES (?, ?, ?, true, DATE '2020-01-01', NULL)",
            [
                (f"S-900{n}", "YY" if n == 6 else "ZZ",
                 "closed" if n == 8 else "open")
                for n in range(1, 9)
            ],
        )
        con.executemany(
            "INSERT INTO raw.market_calendar "
            "(market_code, calendar_date, is_trading_day, feed_expected) "
            "VALUES (?, ?, true, ?)",
            [
                (market, day, not (market == "YY" and day == SYNTH_DS))
                for market in ("ZZ", "YY")
                for day in days(SYNTH_FIRST, SYNTH_LAST)
            ],
        )

        rows: list[tuple] = []
        for day in days(SYNTH_FIRST, SYNTH_DS):
            rows.append(manifest_row("S-9001", day, "ok"))
            rows.append(manifest_row(
                "S-9002", day, "ok" if day <= SYNTH_DS - dt.timedelta(days=9) else "missing"))
            rows.append(manifest_row("S-9003", day, "missing"))
            if day <= SYNTH_DS - dt.timedelta(days=20):
                rows.append(manifest_row("S-9004", day, "ok"))
            rows.append(manifest_row(
                "S-9005", day, "late" if day >= SYNTH_DS - dt.timedelta(days=1) else "missing"))
            if day <= SYNTH_DS - dt.timedelta(days=25):
                rows.append(manifest_row("S-9006", day, "ok"))
            if day <= SYNTH_DS - dt.timedelta(days=30):
                rows.append(manifest_row("S-9008", day, "ok"))
        # S-9004's feed comes back the day after the night under test. A query
        # that takes the whole manifest rather than the window up to the run's
        # date reads these and calls the store quiet.
        for day in days(SYNTH_DS + dt.timedelta(days=1), SYNTH_DS + dt.timedelta(days=5)):
            rows.append(manifest_row("S-9004", day, "ok"))
        con.executemany(
            "INSERT INTO raw.pos_batch_manifest "
            "(batch_id, store_id, business_date, file_name, claimed_row_count, "
            " row_count, status, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    finally:
        con.close()
    return str(path)


@pytest.fixture
def on_estate(estate, monkeypatch) -> str:
    """Point the house library at the synthetic warehouse for one test."""
    monkeypatch.setenv(warehouse.WAREHOUSE_ENV, estate)
    return estate


# --- the world ---------------------------------------------------------------


def test_the_manifest_still_speaks_the_words_this_verifier_reads_it_by():
    """The oracle checks itself against the world before it grades anything.

    Three words, and exactly one of them — the batch that never came — carries
    no row count. If the landed manifest ever holds a fourth, the rest of this
    file is grading a vocabulary the world has moved past and says so here.
    """
    every, delivered = statuses()
    assert len(every) == 3, f"FAIL: the manifest holds the statuses {sorted(every)}"
    assert len(delivered) == 2, (
        f"FAIL: {sorted(delivered)} of {sorted(every)} carry a row count; "
        "the delivered pair is no longer a pair"
    )


def test_the_four_nights_can_still_tell_the_answers_apart():
    """The fixture's own guard. Two of the four nights must hold a store whose
    feed has stopped and two must hold none, or the nights prove nothing."""
    window = lookback()
    for ds in (FIRST_DARK, SECOND_DARK):
        assert silent(ds, window), f"FAIL: {ds}: no store's feed had stopped that night"
    for ds in (QUIET_NIGHT, MARKET_HOLIDAY):
        assert not silent(ds, window), f"FAIL: {ds}: a store's feed had stopped that night"
    assert silent(FIRST_DARK, window) != silent(SECOND_DARK, window), (
        "FAIL: both nights name the same stores, so one of them grades nothing"
    )


def test_an_ordinary_night_escalates_nobody():
    """Every trading store has sent inside the window, so nothing goes to store
    systems. The shipped query answers this night with all 268 stores."""
    check_night(QUIET_NIGHT)


def test_the_night_a_store_had_been_dark_a_fortnight():
    check_night(FIRST_DARK)


def test_the_market_holiday_escalates_nobody_in_the_shut_market():
    """POS-1's calendar check. The one store whose feed had stopped is in a
    market that was not expecting a feed, so the night is quiet."""
    check_night(MARKET_HOLIDAY)


def test_a_night_eleven_weeks_later_names_a_different_store():
    check_night(SECOND_DARK)


def test_running_the_same_night_twice_leaves_one_row_per_store():
    """The escalation is a dated partition like any other. A second run of a
    night replaces it rather than doubling it."""
    escalate(FIRST_DARK)
    once = escalated(FIRST_DARK)
    escalate(FIRST_DARK)
    assert escalated(FIRST_DARK) == once, (
        f"FAIL: {FIRST_DARK}: the second run left {escalated(FIRST_DARK)}"
    )


# --- the estate the world does not hold --------------------------------------


def test_an_estate_the_fixtures_never_mention(on_estate):
    """The same code, eight invented stores, and the answer read off nothing but
    the rules the ticket states: the three whose feed stopped — including the
    one the dimension calls closed, which is the store systems have to confirm —
    and neither the store that has never sent a batch nor the one whose market
    was shut that night."""
    escalate(str(SYNTH_DS))
    assert escalated(str(SYNTH_DS), db=on_estate) == [
        ("S-9002", SYNTH_DS - dt.timedelta(days=9)),
        ("S-9004", SYNTH_DS - dt.timedelta(days=20)),
        ("S-9008", SYNTH_DS - dt.timedelta(days=30)),
    ], f"FAIL: the escalation named {escalated(str(SYNTH_DS), db=on_estate)}"


def test_the_manifest_build_writes_a_word_the_manifest_already_uses(on_estate):
    """The other half of the ticket. The intake stamps a status on every
    store-day it loads, and that status has to be one the manifest's readers
    know. The allowed words are read off the landed table, not typed here.
    """
    from projects.commerce.dags import store_pos_intake

    day = SYNTH_DS + dt.timedelta(days=1)
    query(
        "INSERT INTO raw.pos_sales_header "
        "(pos_txn_id, store_id, register_id, business_date, tender_type, "
        " gross_cents, discount_cents, tax_cents, net_cents, return_flag, void_flag) "
        "SELECT 'T-' || n, 'S-9007', 'R-1', ?::DATE, 'card', 100, 0, 8, 108, false, false "
        "FROM range(3) t(n)",
        [str(day)],
        db=on_estate,
    )
    store_pos_intake.build_manifest(str(day))

    written = query(
        "SELECT status FROM raw.pos_batch_manifest "
        "WHERE store_id = 'S-9007' AND business_date = ?::DATE",
        [str(day)],
        db=on_estate,
    )
    assert written, f"FAIL: the intake wrote no manifest row for {day}"
    every, _ = statuses()
    unknown = sorted({row[0] for row in written} - every)
    assert not unknown, (
        f"FAIL: the intake stamped {unknown}, and the manifest's own words are "
        f"{sorted(every)}"
    )


def test_a_store_day_the_intake_loaded_counts_as_loaded(on_estate):
    """`pos_loaded_stores` decides what the 04:00 catch-up reloads. It reads the
    same column, so it has to agree with what the intake just wrote — otherwise
    the catch-up pulls every store's file back in every night."""
    from projects.commerce.lib import sql

    day = SYNTH_DS + dt.timedelta(days=1)
    loaded = {
        row[0] for row in query(sql.read("pos_loaded_stores"), [str(day)], db=on_estate)
    }
    assert "S-9007" in loaded, (
        f"FAIL: {day}: the store the intake just loaded is not in {sorted(loaded)}"
    )


def test_a_store_that_sent_last_night_is_not_silent_tonight(on_estate):
    """End to end on the estate: load a store-day, then escalate three nights
    later. A store whose batch is in is not a store that has gone quiet, and it
    stays that way whichever of the manifest's words the loader stamps."""
    night = str(SYNTH_DS + dt.timedelta(days=4))
    escalate(night)
    named = {row[0] for row in escalated(night, db=on_estate)}
    assert "S-9007" not in named, (
        f"FAIL: {night}: the escalation named the store whose batch is in: {sorted(named)}"
    )
