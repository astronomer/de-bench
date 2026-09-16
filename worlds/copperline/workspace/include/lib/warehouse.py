"""The warehouse: how to open it, and the two sanctioned ways to write it.

The warehouse is one DuckDB file. `DUCKDB_PATH` names it — `include/data/
copperline.duckdb` — and everything else in this module is relative to that
file. Nothing outside `include/lib/` opens it directly.

**One writer.** DuckDB takes a single writing process. A scheduled run holds
the file for as long as it runs, so anything that only reads must take
`connect(read_only=True)` and leave the live file alone. See `connect`.

**Two writes, and no third.** `delete_insert` replaces one partition of a
table; `write_partition` publishes one partition as a file. A bare `INSERT`
into a dated table is a defect: the second run of an interval puts the rows
in twice. `docs/late-data-policy.md` LD-2 says the same thing in prose — when
late rows arrive for a day that has already been published, the day is
rebuilt from its source and replaces what was there.
"""

from __future__ import annotations

import datetime as dt
import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence

from . import data_dir, workspace_root

if TYPE_CHECKING:  # pragma: no cover - typing only; duckdb is imported lazily
    import duckdb

__all__ = [
    "WAREHOUSE_ENV",
    "warehouse_path",
    "northwave_path",
    "snapshot_path",
    "connect",
    "qualify",
    "delete_insert",
    "write_partition",
    "partition_path",
]

WAREHOUSE_ENV = "DUCKDB_PATH"

#: The frozen `nwv` copy sits beside the live file and the read-only snapshot
#: sits in `snapshot/` under it, so moving the warehouse moves all three
#: together. The snapshot keeps the live file's NAME, because the database's
#: name is its file name and every query qualifies on it — see `qualify`.
NORTHWAVE_FILE = "northwave.duckdb"
SNAPSHOT_DIR = "snapshot"


def warehouse_path() -> Path:
    """The live warehouse file, from `DUCKDB_PATH`.

    A relative value resolves against the repository root, so a task's working
    directory cannot change which warehouse it opens.
    """
    raw = os.environ.get(WAREHOUSE_ENV)
    if not raw:
        raise RuntimeError(
            f"{WAREHOUSE_ENV} is not set, so there is no warehouse to open. "
            "The platform exports it; a local shell has to set it by hand."
        )
    path = Path(raw)
    return path if path.is_absolute() else workspace_root() / path


def northwave_path() -> Path:
    """The frozen Northwave namespace copy, beside the live warehouse."""
    return warehouse_path().with_name(NORTHWAVE_FILE)


def snapshot_path() -> Path:
    """The read-only snapshot: `include/data/snapshot/copperline.duckdb`.

    `scripts/warehouse_snapshot.sh` writes it, and so does
    `connect(read_only=True)` when it is missing or older than the live file.
    """
    live = warehouse_path()
    return live.parent / SNAPSHOT_DIR / live.name


def connect(read_only: bool = False) -> "duckdb.DuckDBPyConnection":
    """Open the house warehouse.

    Attaches the frozen Northwave copy and sets the search path:

        ATTACH 'include/data/northwave.duckdb' AS nwv (READ_ONLY)
        SET search_path = 'nwv,copperline'

    NOTE THE ORDER. An unqualified name resolves against `nwv` FIRST. That was
    the integration's compromise: the acquired reports kept running unqualified
    while their models were ported, and the port is not finished. Twelve tables
    exist in both databases under the same name, and the `nwv` copies stopped
    refreshing when the namespace was frozen on 2025-09-30.

    Qualify every name you write. `select * from staging.orders_enriched` reads
    a copy frozen last September and tells you nothing about it.
    `select * from copperline.staging.orders_enriched` reads today's.

    When the frozen copy is not on disk the attach is skipped, the search path
    is left alone, and every name resolves against the live warehouse.

    `read_only=True` opens a snapshot copy instead of the live file. Use it for
    anything that only reads: the live file takes one writer, and a scheduled
    run holds it, so a second process that wants the live file gets a lock
    error rather than a queue. The snapshot is refreshed from the live file
    when it is missing or older than it, so a read-only connection is never
    more than one run behind. Nothing written through a read-only connection
    reaches the warehouse, because nothing can be written through one. The
    snapshot keeps the live file's name, so `qualify` gives the same answer
    either way and a query written against one runs against the other.

    Close what you open. The connection is a context manager:

        with connect(read_only=True) as con:
            rows = con.execute("select count(*) from copperline.marts.daily_revenue").fetchall()
    """
    import duckdb

    path = _snapshot_of(warehouse_path()) if read_only else warehouse_path()
    con = duckdb.connect(str(path), read_only=read_only)
    frozen = northwave_path()
    if frozen.exists():
        con.execute(f"ATTACH '{frozen}' AS nwv (READ_ONLY)")
        con.execute(f"SET search_path = 'nwv,{path.stem}'")
    return con


def qualify(name: str) -> str:
    """A `schema.table` name, qualified onto the live warehouse.

    `qualify("raw.orders")` gives `copperline.raw.orders`. Use it for every
    name this library or a DAG puts into SQL. An unqualified name resolves
    against the frozen `nwv` copy first — see `connect` — so qualifying is the
    difference between today's rows and a copy that stopped refreshing in
    September. A name that is already qualified is returned as it is.
    """
    live = warehouse_path().stem
    return name if name.startswith(f"{live}.") else f"{live}.{name}"


def _snapshot_of(live: Path) -> Path:
    """Copy the live warehouse to the snapshot when the snapshot is stale."""
    snapshot = snapshot_path()
    if not live.exists():
        raise FileNotFoundError(f"no warehouse at {live}")
    if snapshot.exists() and snapshot.stat().st_mtime >= live.stat().st_mtime:
        return snapshot
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    partial = snapshot.with_name(snapshot.name + ".partial")
    shutil.copyfile(live, partial)
    partial.replace(snapshot)
    return snapshot


def delete_insert(
    table: str,
    partition_col: str,
    ds: str | dt.date,
    rows: Sequence[Mapping[str, Any]] | Sequence[Sequence[Any]],
    *,
    columns: Sequence[str] | None = None,
    con: "duckdb.DuckDBPyConnection | None" = None,
) -> int:
    """Replace one partition of `table` with `rows`. The rerun-safe write.

    THE PARTITION CONTRACT, which is what makes this safe to run twice:

    1. The delete is scoped to ONE partition — `DELETE FROM <table> WHERE
       <partition_col> = <ds>` — and never to the table. A run owns the day it
       is given and no other day's rows are touched, so two runs on two dates
       may write the same table without racing.
    2. The delete and the insert are ONE transaction. A run that dies halfway
       leaves the partition as it found it. There is no window in which the day
       is empty.
    3. `rows` is the WHOLE partition, not the new part of it. Passing only the
       late arrivals deletes the day and writes the tail — which is how a day
       loses the rows nobody noticed were missing.
    4. Every row must carry `ds` in `partition_col`. A row that disagrees with
       the partition it is being written into is a bug in the caller, and this
       function raises rather than write it, because the next run of that day
       would silently delete it again.
    5. Rows are inserted in the order given. Nothing here sorts, dedupes or
       upserts. Where the grain is a key rather than a date, write the merge
       yourself; this function only knows about partitions.
    6. `table` is qualified onto the live warehouse before either statement
       runs, so `"marts.daily_revenue"` writes the live table and never the
       frozen `nwv` copy of the same name.

    `rows` is a list of dicts keyed by column name, or a list of sequences with
    `columns` naming their order. An empty list is legal and means the day is
    genuinely empty: the partition is deleted and nothing replaces it.

    Returns the number of rows written.

    Pass `con` to reuse a connection you already hold, which saves a second
    open on the single-writer file. The delete and the insert are still one
    transaction of their own, so do not call this from inside one. Without
    `con` this opens the live warehouse, commits, and closes it.
    """
    day = _as_date(ds)
    names, tuples = _shape(rows, columns)
    if names and partition_col not in names:
        raise ValueError(
            f"{table}: rows carry no {partition_col!r} column, so there is "
            "nothing to check the partition against"
        )
    if names:
        at = names.index(partition_col)
        wrong = {row[at] for row in tuples if _as_date(row[at]) != day}
        if wrong:
            raise ValueError(
                f"{table}: rows for {sorted(str(v) for v in wrong)} were passed "
                f"to the {day} partition"
            )

    owned = con is None
    con = con or connect()
    target = qualify(table)
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute(f"DELETE FROM {target} WHERE {partition_col} = ?", [day])
        if tuples:
            columns_sql = f" ({', '.join(names)})" if names else ""
            marks = ", ".join("?" for _ in names or tuples[0])
            con.executemany(f"INSERT INTO {target}{columns_sql} VALUES ({marks})", tuples)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        if owned:
            con.close()
    return len(tuples)


def partition_path(layer: str, name: str, ds: str | dt.date) -> Path:
    """Where `write_partition` puts a partition file:
    `include/data/<layer>/<name>_<ds>.csv`."""
    return data_dir() / layer / f"{name}_{_as_date(ds).isoformat()}.csv"


def write_partition(
    layer: str,
    name: str,
    ds: str | dt.date,
    header: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> Path:
    """Publish one partition as a file, and return the path it wrote.

    The path is `include/data/<layer>/<name>_<ds>.csv` and the layout is fixed:
    one comma-separated header line from `header`, then one line per row, in
    the order given. Readers glob on this shape — two sensors in
    `projects/supply/` match a partition by name pattern rather than by task —
    so a partition that lands under another name is a partition nobody reads
    and nothing fails.

    The write is atomic. The file goes to a temporary name in the same
    directory and is renamed over the target, so a reader either sees the
    previous partition or the new one, never half of one. Rerunning an
    interval overwrites that interval's file and touches no other.

    `docs/retention-policy.md` RET-3 governs what happens next: a published
    aggregate partition is immutable. Correcting one is a restatement with
    finance sign-off, not a quiet rewrite.
    """
    import csv

    path = partition_path(layer, name, ds)
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(".csv.partial")
    with partial.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(list(header))
        writer.writerows(rows)
    partial.replace(path)
    return path


def _as_date(value: str | dt.date | dt.datetime) -> dt.date:
    """A partition key as a date. Accepts `{{ ds }}`, a date or a datetime."""
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def _shape(
    rows: Sequence[Mapping[str, Any]] | Sequence[Sequence[Any]],
    columns: Sequence[str] | None,
) -> tuple[list[str], list[tuple]]:
    """Column names and positional tuples, from dicts or from sequences."""
    rows = list(rows)
    if not rows:
        return list(columns or []), []
    if isinstance(rows[0], Mapping):
        names = list(columns or rows[0].keys())
        return names, [tuple(row[n] for n in names) for row in rows]
    if not columns:
        raise ValueError("rows given as sequences need `columns` to name them")
    return list(columns), [tuple(row) for row in rows]
