"""The finance ledger is hand-kept, not generated.

`worlds/copperline/workspace/fixtures/finance/ledger_monthly.csv` is the
independent oracle the recognized-revenue tasks tie against. It was worked out
by hand from `docs/finance-policy.md`; the working is in
`tools/copperline_ledger_workings/`. Two things have to stay true: the file is
byte-stable, and the generator neither writes it nor knows about it. If the
generator ever produced this number, the tie would be the world agreeing with
itself.
"""

import csv
import datetime as dt
import hashlib
import subprocess
import sys

import duckdb
import pytest

from de_bench.tasks import repo_root

GEN = repo_root() / "tools" / "gen_copperline"
WORLD = repo_root() / "worlds" / "copperline"
LEDGER = WORLD / "workspace" / "fixtures" / "finance" / "ledger_monthly.csv"

# Bump this only when the ledger has been worked again by hand and the new
# numbers are written up in tools/copperline_ledger_workings/NOTES.md.
LEDGER_SHA256 = "010135c9e74852aaca6036d5d956503ab4bc29aa5fe15f740d019702f7882067"

TODAY = dt.date(2026, 6, 15)  # worlds/copperline/timeline.yaml

# docs/finance-policy.md, "The five entities".
FIRST_MONTH = {
    "CL-US": "FY2024-P01",
    "CL-GB": "FY2025-P03",
    "CL-IE": "FY2025-P03",
    "CL-DE": "FY2025-P03",
    "CL-MX": "FY2026-P01",
}

COLUMNS = ["entity_code", "fiscal_month", "recognized_cents",
           "posted_on", "preparer", "memo"]


@pytest.fixture(scope="module")
def rows():
    with LEDGER.open(newline="") as fh:
        return list(csv.DictReader(fh))


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A small copperline, built by the real CLI."""
    out = tmp_path_factory.mktemp("copperline")
    db = out / "copperline.duckdb"
    proc = subprocess.run(
        [sys.executable, "-m", "gen_copperline",
         "--timeline", str(WORLD / "timeline.yaml"),
         "--planted", str(WORLD / "planted.yaml"),
         "--db", str(db),
         "--landing", str(out / "landing"),
         "--profile", "small"],
        cwd=repo_root() / "tools", capture_output=True, text=True,
        env={"PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1",
             "PYTHONPATH": str(repo_root() / "tools")},
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    return db


def fifth_business_day(start):
    """REV-8: a month closes on the 5th business day of the one after it."""
    day, seen = start, 0
    while True:
        if day.weekday() < 5:
            seen += 1
            if seen == 5:
                return day
        day += dt.timedelta(days=1)


def closed_months(db):
    """The fiscal months the calendar says had closed by the world's today."""
    con = duckdb.connect(str(db), read_only=True)
    periods = con.execute(
        "SELECT fiscal_year || '-P' || lpad(fiscal_period::varchar, 2, '0'), "
        "       min(cal_date) "
        "FROM raw.fiscal_calendar GROUP BY 1 ORDER BY 2"
    ).fetchall()
    con.close()
    out = {}
    for (label, _), (_, nxt_first) in zip(periods, periods[1:]):
        close_on = fifth_business_day(nxt_first)
        if close_on <= TODAY:
            out[label] = close_on
    return out


def test_the_ledger_parses(rows):
    assert rows, "the ledger is empty"
    assert list(rows[0]) == COLUMNS
    assert len(rows) == 74


def test_every_amount_is_integer_cents(rows):
    for row in rows:
        cents = row["recognized_cents"]
        assert cents == cents.strip(), row
        assert cents.lstrip("-").isdigit(), row
        assert int(cents) != 0, f"a closed month with no revenue: {row}"


def test_one_row_per_entity_month(rows):
    keys = [(r["entity_code"], r["fiscal_month"]) for r in rows]
    assert len(set(keys)) == len(keys), "a duplicated entity month"
    assert {k[0] for k in keys} == set(FIRST_MONTH)


def test_the_months_are_the_calendar_closed_set(rows, world):
    closed = closed_months(world)
    order = {label: i for i, label in enumerate(closed)}
    assert max(closed, key=order.get) == "FY2026-P04"

    for entity, first in FIRST_MONTH.items():
        got = {r["fiscal_month"] for r in rows if r["entity_code"] == entity}
        want = {m for m in closed if order[m] >= order[first]}
        assert got == want, entity

    # An entity has no ledger coverage before the month it began trading, and
    # the open month has no ledger row at all.
    assert not [r for r in rows if r["fiscal_month"] == "FY2026-P05"]


def test_posted_on_is_the_close_date(rows, world):
    closed = closed_months(world)
    for row in rows:
        assert row["posted_on"] == closed[row["fiscal_month"]].isoformat(), row


def test_the_file_is_byte_stable(rows):
    digest = hashlib.sha256(LEDGER.read_bytes()).hexdigest()
    assert digest == LEDGER_SHA256, (
        "the ledger changed. It is hand-kept: regenerating it is not a fix. "
        "Work the months again, write the numbers up in "
        "tools/copperline_ledger_workings/NOTES.md, then move the pin."
    )


def test_the_generator_never_names_the_ledger():
    """World rule 7: the oracle and the world may not share an author."""
    hits = [
        f"{path.relative_to(GEN)}: {line.strip()}"
        for path in GEN.rglob("*")
        if path.is_file() and path.suffix in {".py", ".sql", ".yaml", ".yml"}
        for line in path.read_text(errors="ignore").splitlines()
        if "finance_ledger" in line or "ledger_monthly" in line
    ]
    assert not hits, hits


def test_the_generated_world_holds_no_ledger_table(world):
    con = duckdb.connect(str(world), read_only=True)
    tables = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()}
    con.close()
    assert "finance_ledger" not in tables, (
        "the generator built the ledger. The tie has to be against a number "
        "the world did not compute."
    )
