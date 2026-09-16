"""raw.market_calendar: nine markets, real holidays, and the skip days W2
grades."""

import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from de_bench.tasks import repo_root

WORLD = repo_root() / "worlds" / "copperline"
FACT_RANGE = ("2024-02-04", "2026-06-14")
ALL_SKIP = ["2024-12-25", "2025-12-25", "2026-05-25"]


def _generate(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    db = tmp_path / "copperline.duckdb"
    proc = subprocess.run(
        [sys.executable, "-m", "gen_copperline",
         "--timeline", str(WORLD / "timeline.yaml"),
         "--planted", str(WORLD / "planted.yaml"),
         "--db", str(db),
         "--landing", str(tmp_path / "landing"),
         "--profile", "small"],
        cwd=repo_root() / "tools", capture_output=True, text=True,
        env={"PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1",
             "PYTHONPATH": str(repo_root() / "tools")},
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    return db


@pytest.fixture(scope="module")
def con(tmp_path_factory):
    db = _generate(tmp_path_factory.mktemp("market_calendar") / "a")
    connection = duckdb.connect(str(db), read_only=True)
    yield connection
    connection.close()


def test_one_row_per_market_per_date(con):
    n, markets, lo, hi = con.execute(
        "SELECT count(*), count(DISTINCT market_code), "
        "min(calendar_date), max(calendar_date) FROM raw.market_calendar"
    ).fetchone()
    assert (n, markets, str(lo), str(hi)) == (16443, 9, "2022-01-30", "2027-01-30")

    per_market = con.execute(
        "SELECT DISTINCT count(*) FROM raw.market_calendar GROUP BY market_code"
    ).fetchall()
    assert per_market == [(1827,)]

    codes = [r[0] for r in con.execute(
        "SELECT DISTINCT market_code FROM raw.market_calendar ORDER BY 1"
    ).fetchall()]
    assert codes == ["BR", "CA", "DE", "GB", "ID", "IE", "MX", "PL", "US"]


def test_the_skip_days_are_the_only_ones_and_shut_every_market(con):
    """W2's mechanism: three dates in the fact range where nothing reports."""
    skipped = con.execute(
        "SELECT calendar_date, count(*) FROM raw.market_calendar "
        "WHERE NOT feed_expected AND calendar_date BETWEEN ? AND ? "
        "GROUP BY 1 HAVING count(*) = 9 ORDER BY 1", FACT_RANGE
    ).fetchall()
    assert [str(day) for day, _ in skipped] == ALL_SKIP

    # And no market is shut alone on those days.
    for day in ALL_SKIP:
        reporting = con.execute(
            "SELECT count(*) FROM raw.market_calendar "
            "WHERE calendar_date = ? AND feed_expected", [day]
        ).fetchone()[0]
        assert reporting == 0, day

    # Every other day in the fact range keeps at least one region reporting,
    # which is what makes the publish DAG's partial skip look battle-tested.
    quiet = con.execute(
        "SELECT calendar_date FROM raw.market_calendar "
        "WHERE calendar_date BETWEEN ? AND ? GROUP BY 1 "
        "HAVING bool_and(NOT feed_expected)", FACT_RANGE
    ).fetchall()
    assert [str(day) for (day,) in quiet] == ALL_SKIP


def test_the_graded_holiday_is_named_where_it_is_real(con):
    named = dict(con.execute(
        "SELECT market_code, holiday_name FROM raw.market_calendar "
        "WHERE calendar_date = DATE '2026-05-25'"
    ).fetchall())
    assert named["US"] == "Memorial Day"
    assert named["GB"] == "Spring bank holiday"
    assert named["DE"] == "Whit Monday"
    # Ireland's May bank holiday is the first Monday of May, so it has no
    # holiday that day even though the company skips it everywhere.
    assert named["IE"] is None
    assert all(named[m] is None for m in ("CA", "BR", "MX", "PL", "ID"))
    assert not any(con.execute(
        "SELECT is_trading_day FROM raw.market_calendar "
        "WHERE calendar_date = DATE '2026-05-25'"
    ).fetchall()[0])


def test_christmas_shuts_every_market_in_both_fact_years(con):
    for year in (2024, 2025):
        rows = con.execute(
            "SELECT market_code, holiday_name, is_trading_day, feed_expected "
            "FROM raw.market_calendar WHERE calendar_date = ? ORDER BY 1",
            [f"{year}-12-25"]
        ).fetchall()
        assert len(rows) == 9
        for market, name, trading, feed in rows:
            assert name is not None, (year, market)
            assert "Christmas" in name, (year, market, name)
            assert not trading and not feed, (year, market)


@pytest.mark.parametrize("day, market, name", [
    ("2025-07-04", "US", "Independence Day"),
    ("2025-12-26", "GB", "Boxing Day"),
    ("2024-10-03", "DE", "German Unity Day"),
    ("2025-05-19", "CA", "Victoria Day"),
    ("2026-03-17", "IE", "St Patrick's Day"),
    ("2025-11-11", "PL", "Independence Day"),
])
def test_real_holidays_land_on_their_real_dates(con, day, market, name):
    got = con.execute(
        "SELECT holiday_name, is_trading_day FROM raw.market_calendar "
        "WHERE calendar_date = ? AND market_code = ?", [day, market]
    ).fetchone()
    assert got == (name, False)


def test_flags_agree_with_each_other(con):
    """A day with no feed is never a trading day, and a plain weekday with no
    holiday always trades."""
    assert con.execute(
        "SELECT count(*) FROM raw.market_calendar "
        "WHERE NOT feed_expected AND is_trading_day"
    ).fetchone()[0] == 0
    assert con.execute(
        "SELECT count(*) FROM raw.market_calendar "
        "WHERE holiday_name IS NULL AND NOT is_trading_day "
        "AND dayofweek(calendar_date) BETWEEN 1 AND 5 "
        "AND calendar_date NOT IN (DATE '2024-12-25', DATE '2025-12-25', "
        "DATE '2026-05-25', DATE '2022-12-25', DATE '2023-12-25', "
        "DATE '2026-12-25')"
    ).fetchone()[0] == 0
    # Weekends are non-trading everywhere, but the feeds still land.
    weekend = con.execute(
        "SELECT count(*) FILTER (WHERE is_trading_day), "
        "count(*) FILTER (WHERE NOT feed_expected AND holiday_name IS NULL) "
        "FROM raw.market_calendar WHERE dayofweek(calendar_date) IN (0, 6)"
    ).fetchone()
    assert weekend[0] == 0
    assert weekend[1] == 0


def test_brazil_never_moves_its_clock():
    """Brazil abolished DST in 2019, Mexico in 2022, Indonesia never had it.
    A fix that applies the northern calendar everywhere is wrong for them."""
    sys.path.insert(0, str(repo_root() / "tools"))
    from gen_copperline.extracts import calendars

    for market in ("BR", "MX", "ID"):
        assert calendars.dst_transitions(market) == []
    assert len(calendars.dst_transitions()) == 10
    assert {t[0] for t in calendars.DST_TRANSITIONS if t[2] == "armed"} == {
        "2024-11-03", "2025-11-02"}
    assert {t[0] for t in calendars.DST_TRANSITIONS if t[2] == "graded"} == {
        "2026-03-08", "2026-03-29"}


def test_the_market_calendar_is_deterministic(tmp_path):
    fingerprints = []
    for run in ("a", "b"):
        db = _generate(tmp_path / run)
        connection = duckdb.connect(str(db), read_only=True)
        fingerprints.append(connection.execute(
            "SELECT count(*), sum(hash(t)) FROM raw.market_calendar t"
        ).fetchone())
        connection.close()
    assert fingerprints[0] == fingerprints[1]
    assert fingerprints[0][0] == 16443
