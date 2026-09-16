"""The copperline generator holds its contract: deterministic, wall-clock
free, calendar-correct, and refusing to write a broken world."""

import datetime as dt
import re
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest
import yaml

from de_bench.tasks import repo_root

GEN = repo_root() / "tools" / "gen_copperline"
WORLD = repo_root() / "worlds" / "copperline"


def _generate(tmp_path: Path, planted: str | None = None) -> Path:
    """Run the real CLI at the small profile against the real world config."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    db = tmp_path / "copperline.duckdb"
    planted_path = WORLD / "planted.yaml"
    if planted is not None:
        planted_path = tmp_path / "planted.yaml"
        planted_path.write_text(planted)
    proc = subprocess.run(
        [sys.executable, "-m", "gen_copperline",
         "--timeline", str(WORLD / "timeline.yaml"),
         "--planted", str(planted_path),
         "--db", str(db),
         "--landing", str(tmp_path / "landing"),
         "--profile", "small"],
        cwd=repo_root() / "tools", capture_output=True, text=True,
        env={"PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1",
             "PYTHONPATH": str(repo_root() / "tools")},
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    return db


def _fingerprint(db: Path) -> dict:
    con = duckdb.connect(str(db), read_only=True)
    tables = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema IN ('raw', 'ops') ORDER BY 1, 2"
    ).fetchall()
    out = {}
    for schema, table in tables:
        out[f"{schema}.{table}"] = con.execute(
            f"SELECT count(*), coalesce(sum(hash(t)), 0) FROM {schema}.{table} t"
        ).fetchone()
    con.close()
    return out


def test_no_wall_clock_anywhere():
    """Every 'now' comes from timeline.yaml. A wall-clock read would make the
    world differ between two trials of the same task."""
    pattern = re.compile(r"datetime\.now|date\.today|time\.time\(|utcnow")
    hits = [
        f"{path.relative_to(GEN)}: {line.strip()}"
        for path in GEN.rglob("*.py")
        for line in path.read_text().splitlines()
        if pattern.search(line)
    ]
    assert not hits, hits


def test_generation_is_deterministic(tmp_path):
    first = _fingerprint(_generate(tmp_path / "a"))
    second = _fingerprint(_generate(tmp_path / "b"))
    assert first == second
    assert first, "the generator wrote nothing"


def test_sim_schemas_never_survive(tmp_path):
    con = duckdb.connect(str(_generate(tmp_path)), read_only=True)
    schemas = {r[0] for r in con.execute(
        "SELECT schema_name FROM information_schema.schemata"
    ).fetchall()}
    con.close()
    assert not {s for s in schemas if s.startswith("sim_") or s == "_util"}, schemas
    assert {"raw", "ops"} <= schemas


def test_fiscal_calendar_holds_the_spec(tmp_path):
    con = duckdb.connect(str(_generate(tmp_path)), read_only=True)
    n, lo, hi = con.execute(
        "SELECT count(*), min(cal_date), max(cal_date) FROM raw.fiscal_calendar"
    ).fetchone()
    assert (n, str(lo), str(hi)) == (1827, "2022-01-30", "2027-01-30")

    weeks = dict(con.execute(
        "SELECT fiscal_year, max(fiscal_week) FROM raw.fiscal_calendar GROUP BY 1"
    ).fetchall())
    assert weeks == {"FY2022": 52, "FY2023": 53, "FY2024": 52, "FY2025": 52, "FY2026": 52}

    # The graded 53-week edge: FY2024 W5 (2024-03-03..09) compares to FY2023
    # W6 — the restated week, per finance-policy REV-16 (week W -> week W+1).
    comp_date, comp_week = con.execute(
        "SELECT comp_date_ly, comp_week_ly FROM raw.fiscal_calendar "
        "WHERE cal_date = DATE '2024-03-03'"
    ).fetchone()
    assert (str(comp_date), comp_week) == ("2023-03-05", 6)

    # Year boundaries the spec states outright.
    fy = dict(con.execute(
        "SELECT cal_date, fiscal_year FROM raw.fiscal_calendar WHERE cal_date IN "
        "(DATE '2024-02-03', DATE '2024-02-04', DATE '2026-01-31', DATE '2026-02-01')"
    ).fetchall())
    assert fy == {dt.date(2024, 2, 3): "FY2023", dt.date(2024, 2, 4): "FY2024",
                  dt.date(2026, 1, 31): "FY2025", dt.date(2026, 2, 1): "FY2026"}

    # Every date carries a comp answer except the two honest gaps: FY2022
    # (prior year outside the calendar) and the 53rd week itself.
    gaps = con.execute(
        "SELECT DISTINCT fiscal_year, is_53rd_week FROM raw.fiscal_calendar "
        "WHERE comp_date_ly IS NULL ORDER BY 1, 2"
    ).fetchall()
    assert gaps == [("FY2022", False), ("FY2023", True)]

    # The 53rd week is exactly one week, at the end of FY2023, in period 12.
    n, lo, hi, periods = con.execute(
        "SELECT count(*), min(cal_date), max(cal_date), max(fiscal_period) "
        "FROM raw.fiscal_calendar WHERE is_53rd_week"
    ).fetchone()
    assert (n, str(lo), str(hi), periods) == (7, "2024-01-28", "2024-02-03", 12)
    con.close()


def test_planting_inserts_and_refuses_collisions(tmp_path):
    planted = yaml.dump([
        {"id": "CAL-test-row", "clause": "docs/retail-calendar.md#CAL-1",
         "tasks": ["probe"], "table": "raw.fiscal_calendar", "ds": "2027-02-01",
         "action": "insert", "changes_row_count": True,
         "values": {"cal_date": "2027-01-31", "fiscal_year": "FY2027",
                    "fiscal_quarter": 1, "fiscal_period": 1, "fiscal_week": 1,
                    "week_start": "2027-01-31", "week_end": "2027-02-06",
                    "day_of_fiscal_week": 1, "is_53rd_week": False}},
    ])
    con = duckdb.connect(str(_generate(tmp_path, planted=planted)), read_only=True)
    fy = con.execute(
        "SELECT fiscal_year FROM raw.fiscal_calendar WHERE cal_date = DATE '2027-01-31'"
    ).fetchone()
    con.close()
    assert fy == ("FY2027",)

    # Two rows on the same target must refuse the whole build.
    doc = yaml.safe_load(planted)
    doc.append({**doc[0], "id": "CAL-test-row-2"})
    with pytest.raises(AssertionError, match="touch the same target"):
        _generate(tmp_path / "again", planted=yaml.dump(doc))


def test_a_planted_row_without_a_task_or_clause_refuses(tmp_path):
    for missing in ("tasks", "clause"):
        row = {"id": "X-1", "clause": "docs/x.md#X-1", "tasks": ["t"],
               "table": "raw.fiscal_calendar", "ds": "2026-01-01",
               "values": {"cal_date": "2027-01-31"}}
        row.pop(missing)
        with pytest.raises(AssertionError, match=f"names no {missing.rstrip('s')}"):
            _generate(tmp_path / missing, planted=yaml.dump([row]))
