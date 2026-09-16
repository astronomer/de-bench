"""The frozen `nwv` sibling: it lands, it traps, and it stops at the freeze.

`include/lib/warehouse.connect()` attaches `northwave.duckdb` READ_ONLY as
`nwv` and puts it FIRST on the search path, so an unqualified table name reads
a copy of Northwave's book that stopped refreshing on 2025-09-30. Everything
here is asserted through that arrangement rather than against the generator's
internals: the file is opened the way the world opens it, and the trap either
fires or it does not.

The world is built through the real CLI, twice, because where the file lands
is derived from the connection the CLI hands the generator — a helper that
took the path as an argument would test something the world never runs.
"""

import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from de_bench.tasks import repo_root

WORLD = repo_root() / "worlds" / "copperline"

#: The freeze. `worlds/copperline/timeline.yaml` calls it `nwv_frozen`.
FROZEN = "2025-09-30"

#: What chapter 05 promises: twelve tables exist in both databases.
SHARED_NAMES = {
    "customers", "fiscal_calendar", "inventory_snapshots", "invoices",
    "order_lines", "order_status_history", "orders", "payment_terms",
    "pos_sales_daily", "product_categories", "stores", "wms_movements",
}

#: The two counts the trap is measured by: Northwave's account book against
#: Copperline's. Neither scales with the profile.
NWV_CUSTOMERS = 1400
COPPERLINE_CUSTOMERS = 4000


def _generate(tmp_path: Path) -> Path:
    """Run the real CLI at the small profile against the real world config."""
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


def _attached(db: Path) -> duckdb.DuckDBPyConnection:
    """The connection `warehouse.connect()` builds: the frozen copy attached
    READ_ONLY as `nwv`, and `nwv` first on the search path."""
    frozen = db.with_name("northwave.duckdb")
    con = duckdb.connect(str(db), read_only=True)
    con.execute(f"ATTACH '{frozen}' AS nwv (READ_ONLY)")
    con.execute(f"SET search_path = 'nwv,{db.stem}'")
    return con


def _frozen_tables(con) -> list[str]:
    return [r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE database_name = 'nwv' AND schema_name = 'raw' ORDER BY 1"
    ).fetchall()]


def _fingerprint(db: Path) -> dict:
    con = duckdb.connect(str(db.with_name("northwave.duckdb")), read_only=True)
    out = {
        table: con.execute(
            f'SELECT count(*), coalesce(sum(hash(t)), 0) FROM raw."{table}" t'
        ).fetchone()
        for (table,) in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'raw'"
        ).fetchall()
    }
    con.close()
    return out


@pytest.fixture(scope="module")
def worlds(tmp_path_factory):
    """Two builds of the same config, and a connection onto the first."""
    root = tmp_path_factory.mktemp("nwv")
    first, second = _generate(root / "a"), _generate(root / "b")
    con = _attached(first)
    yield con, first, second
    con.close()


def test_the_frozen_copy_lands_beside_the_warehouse(worlds):
    """`warehouse.northwave_path()` is the live file's directory and this
    name, so the generator has to derive the same place from the connection
    it was given — the CLI's `--db` and the image bake alike."""
    _con, db, _second = worlds
    frozen = db.with_name("northwave.duckdb")
    assert frozen.exists(), sorted(p.name for p in db.parent.iterdir())
    assert frozen.parent == db.parent


def test_the_search_path_trap_fires(worlds):
    """The whole point of the chapter: an unqualified name reads the frozen
    copy, a qualified one reads today's warehouse, and both queries run."""
    con, _db, _second = worlds
    assert con.execute("SELECT count(*) FROM raw.customers").fetchone()[0] \
        == NWV_CUSTOMERS
    assert con.execute(
        "SELECT count(*) FROM copperline.raw.customers").fetchone()[0] \
        == COPPERLINE_CUSTOMERS

    # Not one table: every shared name answers from the frozen copy first.
    for table in sorted(SHARED_NAMES):
        stale = con.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
        frozen = con.execute(
            f"SELECT count(*) FROM nwv.raw.{table}").fetchone()[0]
        assert stale == frozen, table

    # And the trap is silent: a money query written for Copperline runs and
    # answers with Northwave's numbers rather than raising.
    period = ("WHERE local_order_date BETWEEN DATE '2025-08-01' "
              "AND DATE '2025-08-31' AND order_status = 'fulfilled'")
    stale = con.execute(
        f"SELECT sum(grand_total_cents) FROM raw.orders {period}").fetchone()[0]
    live = con.execute(
        f"SELECT sum(grand_total_cents) FROM copperline.raw.orders {period}"
    ).fetchone()[0]
    assert stale and live and stale != live


def test_twelve_tables_exist_in_both_databases(worlds):
    """Chapter 05 says twelve. The other frozen tables carry Northwave's own
    names, so an unqualified read of one of those is unambiguous."""
    con, _db, _second = worlds
    frozen = set(_frozen_tables(con))
    live = {r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE database_name = 'copperline' AND schema_name = 'raw'"
    ).fetchall()}
    assert frozen & live == SHARED_NAMES
    assert len(SHARED_NAMES) == 12
    assert frozen - live, "the copy carries no book of its own"


def test_a_shared_name_carries_the_copperline_shape(worlds):
    """A column the frozen copy holds must exist in the Copperline table
    under the same type. Anything else and the unqualified query raises,
    which is a trap that announces itself."""
    con, _db, _second = worlds
    for table in sorted(SHARED_NAMES):
        frozen = dict(con.execute(
            "SELECT column_name, data_type FROM duckdb_columns() "
            "WHERE database_name = 'nwv' AND table_name = ?", [table]
        ).fetchall())
        live = dict(con.execute(
            "SELECT column_name, data_type FROM duckdb_columns() "
            "WHERE database_name = 'copperline' AND schema_name = 'raw' "
            "AND table_name = ?", [table]
        ).fetchall())
        assert frozen, table
        assert {c: t for c, t in frozen.items() if live.get(c) != t} == {}, table


def test_nothing_in_the_copy_happens_after_the_freeze(worlds):
    """The namespace froze on 2025-09-30 and never refreshed, so every clock
    the file carries stops on or before that date."""
    con, _db, _second = worlds
    stamps = con.execute(
        "SELECT table_name, column_name FROM duckdb_columns() "
        "WHERE database_name = 'nwv' AND data_type = 'TIMESTAMP' ORDER BY 1, 2"
    ).fetchall()
    assert stamps, "the copy carries no clock at all"
    for table, column in stamps:
        late = con.execute(
            f'SELECT count(*) FROM nwv.raw."{table}" '
            f"WHERE \"{column}\"::DATE > DATE '{FROZEN}'"
        ).fetchone()[0]
        assert late == 0, f"{table}.{column}"

    # The load clock stops ON the freeze, not before it: the last refresh ran
    # that night, which is what makes the copy nine months stale and not ten.
    last = con.execute("SELECT max(loaded_at) FROM nwv.raw.orders").fetchone()[0]
    assert str(last)[:10] == FROZEN


def test_the_merge_answer_key_never_reaches_the_copy(worlds):
    """`sim_nwv.accounts.pair_role` names the 300 true merge pairs and the 60
    decoys outright, and `nwv_seq` gives the same answer by ordinal. NW-214
    is only a task while both stay out of the shipped file."""
    con, _db, _second = worlds
    leaked = con.execute(
        "SELECT table_name, column_name FROM duckdb_columns() "
        "WHERE database_name = 'nwv' "
        "AND column_name IN ('pair_role', 'nwv_seq', 'trade_seq')"
    ).fetchall()
    assert leaked == []


def test_two_builds_write_the_same_frozen_copy(worlds):
    """Same config, same world — the sibling file included."""
    _con, first, second = worlds
    a, b = _fingerprint(first), _fingerprint(second)
    assert a == b
    assert len(a) == len(SHARED_NAMES) + 8


def test_the_world_ships_the_sibling(worlds):
    """world.yaml has to carry the second file, or the bake writes it and no
    trial ever sees it."""
    import yaml

    cfg = yaml.safe_load((WORLD / "world.yaml").read_text())
    data = {d["to"]: d["from"] for d in cfg["runtime"]["workspace_data"]}
    warehouse = cfg["runtime"]["env"]["DUCKDB_PATH"]
    assert data["include/data/northwave.duckdb"] == "/opt/copperline/northwave.duckdb"
    # Beside the warehouse, which is where warehouse.northwave_path() looks.
    assert str(Path(warehouse).parent) == str(Path("include/data/northwave.duckdb").parent)
