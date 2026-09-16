"""The market seed after GTM-77: the right markets, the right attributes, and a
Monday rebuild that agrees with the file in the repo.

Nothing here holds an authored currency or entity. Every attribute is read from
`raw.market_config` in the warehouse the scorer rebuilt from the image, which is
the same table the ticket's answer has to be read from. The only authored thing
is the SET of market codes the seed must end up holding, and it is authored
twice over: once as the five that shipped plus the four the ticket names, and
once as a cross-check against the config table, so that a world change that
moves the market list fails this verifier loudly instead of grading a stale
list.

The rebuild test rebuilds the seed from the blueprint's own keys rather than by
importing `projects/growth/dags/kinds.py`. That module is inside the tree under
test; an oracle that imports it grades the answer with the answer.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import duckdb
import yaml

# The scorer lays the tree at /work and runs pytest there. The override exists
# so this file can be exercised against a tree on a laptop; nothing in a trial
# or in scoring sets it.
WORKDIR = Path(os.environ.get("DE_BENCH_WORKDIR") or "/work")
SEED = WORKDIR / "dbt" / "copperline_analytics" / "seeds" / "markets.csv"
BLUEPRINT = WORKDIR / "projects" / "growth" / "dags" / "gro_market_seed_build.dag.yaml"
DB = WORKDIR / os.environ.get("DUCKDB_PATH", "include/data/copperline.duckdb")

#: The five markets the seed shipped with, and the four the ticket opens. The
#: ticket authors this set and nothing else: which markets belong in the seed is
#: the ask, what each one bills in is the world's to say.
SHIPPED = ("CA", "DE", "GB", "IE", "US")
OPENED = ("BR", "ID", "MX", "PL")
EXPECTED_CODES = sorted(SHIPPED + OPENED)

#: The seed's header. `kinds.seed_file` calls a seed's header part of its
#: contract and refuses to take it from whatever the table happens to hold, so
#: it is pinned here as well as compared against the blueprint.
HEADER = ["market_code", "currency", "entity"]


def config() -> dict[str, tuple[str, str]]:
    """Every market the market-setup export records, with what it bills in."""
    con = duckdb.connect(str(DB), read_only=True)
    try:
        rows = con.execute(
            "SELECT market_code, billing_currency, entity_code, is_active "
            "FROM raw.market_config"
        ).fetchall()
    finally:
        con.close()
    return {code: (currency, entity) for code, currency, entity, active in rows if active}


def seed() -> tuple[list[str], list[tuple[str, ...]]]:
    """The committed seed: its header, and its rows as strings."""
    assert SEED.exists(), f"{SEED} is gone; the pricing models build from it"
    with SEED.open(encoding="utf-8-sig", newline="") as handle:
        table = [row for row in csv.reader(handle) if any(cell.strip() for cell in row)]
    assert table, f"{SEED} is empty"
    header = [cell.strip() for cell in table[0]]
    rows = [tuple(cell.strip() for cell in row) for row in table[1:]]
    return header, rows


def seed_step() -> dict:
    """The step of the weekly job that writes `seeds/markets.csv`."""
    assert BLUEPRINT.exists(), f"{BLUEPRINT} is gone; nothing rebuilds the seed"
    document = yaml.safe_load(BLUEPRINT.read_text(encoding="utf-8")) or {}
    steps = document.get("steps") or {}
    writers = [
        dict(step)
        for step in steps.values()
        if isinstance(step, dict)
        and step.get("blueprint") == "seed_file"
        and str(step.get("name", "")).strip() == "markets"
    ]
    assert len(writers) == 1, (
        f"{BLUEPRINT.name} holds {len(writers)} seed_file step(s) writing markets, wanted 1"
    )
    return writers[0]


def rebuilt() -> tuple[list[str], list[tuple[str, ...]]]:
    """What the weekly job would write, run against the warehouse as it stands.

    The same three keys `seed_file` reads — the source table, the column
    mapping and the filter — assembled into the same query.
    """
    step = seed_step()
    columns = dict(step["columns"])
    source = str(step["source"])
    where = step.get("where")
    order_by = list(step.get("order_by") or [])
    for label, value in (("source", source), ("where", where)):
        assert "{{" not in str(value or ""), (
            f"the {label} key is templated ({value!r}); this verifier renders no template"
        )
    sql = "SELECT " + ", ".join(f"{src} AS {dst}" for src, dst in columns.items())
    sql += f" FROM {source}"
    if where:
        sql += f" WHERE {where}"
    if order_by:
        sql += " ORDER BY " + ", ".join(order_by)
    con = duckdb.connect(str(DB), read_only=True)
    try:
        rows = con.execute(sql).fetchall()
    finally:
        con.close()
    return list(columns.values()), [tuple(str(cell) for cell in row) for row in rows]


def test_the_market_set_the_ticket_names_is_still_the_whole_market_book():
    """The oracle checks itself against the world before it grades anything.

    If a later change opens a tenth market, the set this verifier expects stops
    being the whole of `raw.market_config` and this test says so, rather than
    the seed tests quietly grading a list the world has moved past.
    """
    assert sorted(config()) == EXPECTED_CODES


def test_the_seed_lists_every_market_that_trades():
    """The four opened markets are in the seed, and the five already there stayed."""
    _, rows = seed()
    codes = sorted(row[0] for row in rows)
    assert codes == EXPECTED_CODES, f"the seed lists {codes}"


def test_every_seeded_market_bills_what_the_market_setup_record_says_it_bills():
    """The currency and the entity on every row come from the market-setup
    export, which is the record for both. This is the whole of the ticket: the
    seed is a three-column file and two of the three columns are facts about the
    business that the business has already written down."""
    recorded = config()
    header, rows = seed()
    wrong = []
    for row in rows:
        row_by_column = dict(zip(header, row))
        code = row_by_column.get("market_code")
        got = (row_by_column.get("currency"), row_by_column.get("entity"))
        want = recorded.get(code)
        if want is not None and got != want:
            wrong.append(f"{code} seeded as {got}, recorded as {want}")
    assert not wrong, "; ".join(wrong)


def test_the_seed_keeps_the_header_the_job_writes():
    """A seed's header is part of its contract. It has to stay the three names
    the models read, and it has to be the header the weekly job writes, or the
    first rebuild changes the file's shape."""
    header, _ = seed()
    assert header == HEADER, f"the seed's header is {header}"
    job_header, _ = rebuilt()
    assert job_header == HEADER, f"the job would write the header {job_header}"


def test_mondays_rebuild_writes_the_same_seed_back():
    """The job that rebuilds the seed every Monday reproduces what is in the
    repo. A seed edited by hand, over a job that still selects the five markets
    it always selected, is undone on the next run and nobody sees it happen."""
    _, committed = seed()
    _, weekly = rebuilt()
    assert sorted(weekly) == sorted(committed), (
        f"the job would write {sorted(weekly)}, the repo holds {sorted(committed)}"
    )
