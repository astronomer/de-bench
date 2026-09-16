"""The rate intake after FIN-524: the load agrees with the rate table, and the
coverage step counts the day's currencies instead of dying on a column nobody
ever landed.

NOTHING HERE IS AUTHORED. The column names and the column types the load must
declare are read off `raw.fx_rates` in the warehouse the scorer rebuilt from the
image, and the number the coverage row must carry is counted from the same table
in the same session. The only authored things are the dag_id, the table names
the ticket says are not moving, and the name of the coverage column the ticket
says to keep.

The coverage step is not run through `include/lib/blueprint/kinds/rollup.py`.
That module is inside the tree under test, and an oracle that imports it grades
the answer with the answer. The four keys the kind reads — group_by, agg,
column, where — are assembled here into the same query the kind assembles, and
run read-only against the warehouse. Nothing in this file writes.

The first test is the oracle checking itself against the world: if a later
change moves the rate table's shape, it says so rather than letting the rest of
the file grade a stale expectation.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import yaml

# The scorer lays the tree at /work and runs pytest there. The override exists
# so this file can be exercised against a tree on a laptop; nothing in a trial
# or in scoring sets it.
WORKDIR = Path(os.environ.get("DE_BENCH_WORKDIR") or "/work")
DAG_DIR = WORKDIR / "projects" / "finance" / "dags"
DB = WORKDIR / os.environ.get("DUCKDB_PATH", "include/data/copperline.duckdb")

#: The job under the ticket, the table it lands, and the table it rolls up.
#: All three are stated in the ticket as fixed.
DAG_ID = "fin_fx_rates_intake"
RATE_TABLE = "raw.fx_rates"
COVERAGE_TABLE = "ops.fx_rate_coverage"

#: The coverage column the ticket says to keep, because it is what the coverage
#: table is for.
COVERAGE_COLUMN = "currency_count"

#: The aggregates `rollup` offers and the SQL each becomes. Copied rather than
#: imported, for the reason in the module docstring — `include/lib/` is in the
#: tree under test. It is protected shared code, so this list is stable.
AGGREGATES = {
    "sum": "sum({column})",
    "count": "count({column})",
    "count_distinct": "count(DISTINCT {column})",
    "avg": "avg({column})",
    "min": "min({column})",
    "max": "max({column})",
    "median": "median({column})",
}

#: Three ordinary rate dates, none of them named in the ticket, none inside a
#: reserved window or the never-grade tail. The rate feed sends every currency
#: every day, so these are three draws from the same shape rather than three
#: edges — they are here so a fix that happens to work on one day has to work on
#: a day either side of it too.
DATES = ("2025-11-03", "2026-03-16", "2026-04-20")


def connect() -> duckdb.DuckDBPyConnection:
    assert DB.exists(), f"{DB} is not there; the rate table is what this grades against"
    return duckdb.connect(str(DB), read_only=True)


def table_columns(table: str) -> dict[str, str]:
    """Every column of a warehouse table, with its type, in the table's order."""
    schema, _, bare = table.rpartition(".")
    con = connect()
    try:
        rows = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, bare],
        ).fetchall()
    finally:
        con.close()
    return {name: kind for name, kind in rows}


def normalised(declared: str) -> str:
    """A SQL type as DuckDB itself spells it, so `STRING` and `VARCHAR` are one
    answer and a type the database cannot read fails with its own message."""
    con = connect()
    try:
        return str(con.execute(f"SELECT typeof(CAST(NULL AS {declared}))").fetchone()[0])
    finally:
        con.close()


def blueprint() -> tuple[Path, dict]:
    """The rendered job that carries the dag_id, wherever in the team's folder
    it sits. Found by dag_id rather than by filename, so a rename is not a
    failure — but the job has to still be a rendered one, which is the ticket's
    last rule."""
    assert DAG_DIR.is_dir(), f"{DAG_DIR} is gone"
    found = []
    for path in sorted(DAG_DIR.glob("*.dag.yaml")):
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:  # a broken yaml takes the whole folder down
            raise AssertionError(f"{path.name} does not parse: {exc}") from exc
        if isinstance(document, dict) and document.get("dag_id") == DAG_ID:
            found.append((path, document))
    assert len(found) == 1, (
        f"{len(found)} rendered job(s) in {DAG_DIR} carry dag_id {DAG_ID}, wanted 1"
    )
    return found[0]


def steps_of(kind: str) -> list[dict]:
    """Every step of the job that uses one blueprint kind, name included."""
    _, document = blueprint()
    steps = document.get("steps") or {}
    assert isinstance(steps, dict) and steps, f"{DAG_ID} declares no steps"
    out = []
    for name, step in steps.items():
        if isinstance(step, dict) and step.get("blueprint") == kind:
            out.append({"name": name, **step})
    return out


def only_step(kind: str, key: str, value: str) -> dict:
    """The one step of a kind that names `value` under `key`."""
    matches = [s for s in steps_of(kind) if str(s.get(key, "")).strip() == value]
    assert len(matches) == 1, (
        f"{DAG_ID} has {len(matches)} {kind} step(s) with {key}={value}, wanted 1; "
        f"the {kind} steps are {[(s['name'], s.get(key)) for s in steps_of(kind)]}"
    )
    return matches[0]


def coverage_sql(step: dict, date: str) -> str:
    """The query `rollup` would run for one day, from the step's own keys.

    The same assembly the kind does: group by what the step groups by, aggregate
    what it aggregates, and scope the read to the partition when the source
    carries the partition column.
    """
    source = str(step["source"])
    group_by = [str(c) for c in (step.get("group_by") or [])]
    agg = str(step.get("agg", "sum"))
    column = str(step["column"])
    as_column = str(step.get("as") or f"{agg}_{column}".replace("*", "rows"))
    partition_col = str(step.get("partition_col", "ds"))
    where = step.get("where")
    assert agg in AGGREGATES, f"the coverage step asks for agg={agg!r}, which the kind does not offer"
    assert group_by, "the coverage step groups by nothing"
    for label, value in (("source", source), ("where", where)):
        assert "{{" not in str(value or ""), (
            f"the coverage step's {label} is templated ({value!r}); this reads no template"
        )
    filters = []
    if partition_col in table_columns(source):
        filters.append(f"{partition_col} = '{date}'")
    if where:
        filters.append(f"({where})")
    grouped = ", ".join(group_by)
    sql = f"SELECT {grouped}, {AGGREGATES[agg].format(column=column)} AS {as_column} FROM {source} "
    if filters:
        sql += f"WHERE {' AND '.join(filters)} "
    return sql + f"GROUP BY {grouped} ORDER BY {grouped}"


def currencies_on(date: str) -> int:
    """How many currencies the rate table carries for one day."""
    con = connect()
    try:
        return int(con.execute(
            f"SELECT count(DISTINCT currency_code) FROM {RATE_TABLE} WHERE rate_date = ?",
            [date],
        ).fetchone()[0])
    finally:
        con.close()


def test_the_rate_table_still_has_the_shape_this_grades_against():
    """The oracle checks itself against the world before it grades anything.

    Everything below is read off `raw.fx_rates`: the columns the load must
    declare, and the currency count the coverage row must carry. If a later
    change moves that table, this says so rather than letting the rest grade an
    expectation the world has left behind.
    """
    columns = table_columns(RATE_TABLE)
    assert columns, f"{RATE_TABLE} is not in the warehouse"
    assert "rate_date" in columns and "currency_code" in columns, (
        f"{RATE_TABLE} carries {sorted(columns)}"
    )
    assert "base_currency" not in columns, (
        f"{RATE_TABLE} now carries a base_currency column; this ticket is about it not existing"
    )
    for date in DATES:
        assert currencies_on(date) > 1, (
            f"{date} carries {currencies_on(date)} currency; a coverage count over one row grades nothing"
        )


def test_the_job_is_still_a_rendered_one_with_its_four_steps():
    """The ticket's last rule. Hand-writing the job, dropping the coverage step
    or pointing it somewhere else are three ways to make the morning quiet
    without landing anything, and each one lands here."""
    path, document = blueprint()
    steps = document.get("steps") or {}
    kinds = sorted(
        str(step.get("blueprint"))
        for step in steps.values()
        if isinstance(step, dict)
    )
    assert kinds == ["csv_intake", "partition_export", "rollup", "sensor_wait"], (
        f"{path.name} renders {kinds}"
    )
    only_step("csv_intake", "table", RATE_TABLE)
    only_step("rollup", "target", COVERAGE_TABLE)


def test_the_load_declares_the_columns_the_rate_table_carries():
    """The whole of the first fault. `columns` types the read, so the names in
    it are the names the file has to carry, and the file's names are the table's
    names — nothing else lands. The two the job declares today are on neither.

    The order is not graded. The set is, and so is the type of every column.
    """
    step = only_step("csv_intake", "table", RATE_TABLE)
    declared = step.get("columns")
    assert isinstance(declared, dict) and declared, (
        "the load declares no columns; without them the sniffer guesses and the "
        "guess is not stable across DuckDB minors"
    )
    recorded = table_columns(RATE_TABLE)
    assert sorted(declared) == sorted(recorded), (
        f"the load declares {sorted(declared)}, the table carries {sorted(recorded)}"
    )
    wrong = [
        f"{name} declared {kind}, the table holds {recorded[name]}"
        for name, kind in declared.items()
        if normalised(str(kind)) != normalised(recorded[name])
    ]
    assert not wrong, "; ".join(wrong)


def test_the_load_still_replaces_the_day_it_loads():
    """The ticket's second rule, and the loader's own trap: replace without a
    partition column falls back to append with one line at INFO, and the file
    gets re-sent."""
    step = only_step("csv_intake", "table", RATE_TABLE)
    assert step.get("mode") == "replace", f"the load runs mode={step.get('mode')!r}"
    partition_col = str(step.get("partition_col") or "")
    recorded = table_columns(RATE_TABLE)
    assert partition_col in recorded, (
        f"the load replaces on {partition_col!r}, which {RATE_TABLE} does not carry"
    )
    assert normalised(recorded[partition_col]) == "DATE", (
        f"the load replaces on {partition_col}, which is {recorded[partition_col]}, not a date"
    )


def test_the_coverage_step_reads_the_day_and_not_the_whole_table():
    """`rollup` scopes the read to the partition only when the step names a
    partition column the source carries. Without one the coverage row holds an
    all-time figure, which is not what anybody reading it for one morning
    wants."""
    step = only_step("rollup", "target", COVERAGE_TABLE)
    partition_col = str(step.get("partition_col") or "ds")
    assert partition_col in table_columns(RATE_TABLE), (
        f"the coverage step scopes on {partition_col!r}, which {RATE_TABLE} does not carry, "
        "so it would read the table whole"
    )


def test_the_four_steps_read_the_same_day():
    """The rate date is the day before the run and every step that names it has
    to name the same one. A step left on the run's own day reads a morning the
    file has not landed for."""
    _, document = blueprint()
    steps = document.get("steps") or {}
    named = {
        name: str(step["partition_value"])
        for name, step in steps.items()
        if isinstance(step, dict) and "partition_value" in step
    }
    assert len(named) >= 3, f"only {sorted(named)} name a partition value"
    assert len(set(named.values())) == 1, f"the steps read different days: {named}"


def test_the_coverage_row_carries_the_number_of_currencies_the_day_brought():
    """The whole of the second fault, and the one the first fault hides: the
    step groups by a column the rate table has never carried, so it raises the
    moment the load ahead of it starts working.

    What replaces it has to leave `currency_count` meaning the number of
    currencies in the morning's file. A grouping that splits the day puts a 1 on
    every row and tells a reader nothing; a currency named in the step counts
    only the currencies somebody typed.
    """
    step = only_step("rollup", "target", COVERAGE_TABLE)
    assert str(step.get("as") or "") == COVERAGE_COLUMN, (
        f"the coverage step writes {step.get('as')!r}; the ticket keeps {COVERAGE_COLUMN}"
    )
    recorded = table_columns(RATE_TABLE)
    unknown = [c for c in (step.get("group_by") or []) if str(c) not in recorded]
    assert not unknown, f"the coverage step groups by {unknown}, which {RATE_TABLE} does not carry"
    assert str(step["column"]) in recorded, (
        f"the coverage step counts {step['column']!r}, which {RATE_TABLE} does not carry"
    )

    wrong = []
    for date in DATES:
        want = currencies_on(date)
        con = connect()
        try:
            rows = con.execute(coverage_sql(step, date)).fetchall()
        finally:
            con.close()
        if not rows:
            wrong.append(f"{date}: the coverage step writes no row")
            continue
        counted = [row[-1] for row in rows]
        if any(value != want for value in counted):
            wrong.append(f"{date}: coverage says {counted}, the day carries {want} currencies")
    assert not wrong, "; ".join(wrong)
