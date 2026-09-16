"""The checks every finance mart runs after it rebuilds.

Four of them, and each one has failed in production at least once. They are
here rather than in each DAG because a check that lives in six DAG files is a
check that is six different checks within a year.

Nothing here writes. `include.lib.warehouse` owns the writes and these read
through the snapshot, so a check never queues behind the build it is checking.
"""

from __future__ import annotations

from typing import Sequence

from include.lib import warehouse

__all__ = ["has_partition", "assert_integer_cents", "assert_row_count",
           "assert_total_matches", "publish_partition"]

#: Warehouse types a money column may never have. A float in cents is a
#: rounding error waiting for a close.
FLOAT_TYPES = ("FLOAT", "REAL", "FLOAT4", "DOUBLE", "FLOAT8", "DECIMAL", "NUMERIC")


def has_partition(table: str, partition_col: str, partition_value: str) -> bool:
    """Whether a table holds rows for a day. What a wait pokes on.

    Reads the snapshot, so the wait clears one snapshot refresh after the rows
    land rather than the instant they do. That is the cost of not queueing
    behind the writer, and for a mart built two hours earlier it is free.
    """
    schema, _, bare = table.rpartition(".")
    with warehouse.connect(read_only=True) as con:
        exists = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, bare],
        ).fetchone()
        if not exists:
            return False
        found = con.execute(
            f"SELECT 1 FROM {warehouse.qualify(table)} WHERE {partition_col} = ? LIMIT 1",
            [partition_value],
        ).fetchone()
    return bool(found)


def assert_integer_cents(table: str, columns: Sequence[str]) -> int:
    """Every named column is an integer type. Raises when one is not.

    A money column that has become a double is the failure that hides longest:
    the totals still look right, the tie is out by fractions of a cent a row,
    and the difference only shows up at the month's scale.
    """
    schema, _, bare = table.rpartition(".")
    with warehouse.connect(read_only=True) as con:
        types = dict(con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, bare],
        ).fetchall())
    wrong = {
        name: types[name] for name in columns
        if name in types and str(types[name]).split("(")[0].upper() in FLOAT_TYPES
    }
    if wrong:
        raise ValueError(
            f"{table}: money is integer cents, and "
            + ", ".join(f"{name} is {kind}" for name, kind in sorted(wrong.items()))
        )
    return len(columns)


def assert_row_count(table: str, partition_col: str, partition_value: str,
                     *, at_least: int = 1) -> int:
    """The day landed at least this many rows. Returns the count.

    An empty partition is the quiet failure: the build succeeded, the table
    exists, and the day has nothing in it.
    """
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT count(*) FROM {warehouse.qualify(table)} WHERE {partition_col} = ?",
            [partition_value],
        ).fetchone()[0]
    if rows < at_least:
        raise ValueError(
            f"{table}: {rows} row(s) for {partition_value}, and {at_least} was the floor"
        )
    return int(rows)


def assert_total_matches(table: str, column: str, source_table: str,
                         source_column: str, partition_col: str,
                         partition_value: str, *,
                         source_partition_col: str | None = None,
                         tolerance_cents: int = 0) -> dict:
    """One day's total against the source it was built from, in cents.

    Returns both totals and the difference. Raises when the difference is
    outside the tolerance, which is zero unless a caller has a reason and can
    say what it is.
    """
    source_partition_col = source_partition_col or partition_col
    with warehouse.connect(read_only=True) as con:
        built = con.execute(
            f"SELECT coalesce(sum({column}), 0) FROM {warehouse.qualify(table)} "
            f"WHERE {partition_col} = ?",
            [partition_value],
        ).fetchone()[0]
        source = con.execute(
            f"SELECT coalesce(sum({source_column}), 0) FROM "
            f"{warehouse.qualify(source_table)} WHERE {source_partition_col} = ?",
            [partition_value],
        ).fetchone()[0]
    difference = int(built) - int(source)
    if abs(difference) > tolerance_cents:
        raise ValueError(
            f"{table}.{column} is {built} for {partition_value} and "
            f"{source_table}.{source_column} is {source}: {difference} cents apart"
        )
    return {"built_cents": int(built), "source_cents": int(source),
            "difference_cents": difference}


def publish_partition(table: str, partition_col: str, partition_value: str,
                      name: str, *, layer: str = "marts",
                      order_by: Sequence[str] = ()) -> str:
    """Write one day of a mart out as a file, and return the path.

    The layout is `include/data/<layer>/<name>_<ds>.csv` and it is fixed:
    readers elsewhere in the estate match these files by name pattern rather
    than by task, so a partition that lands under another name is a partition
    nobody reads and nothing fails. `docs/lineage.md` lists who reads what.
    """
    order = f" ORDER BY {', '.join(order_by)}" if order_by else ""
    with warehouse.connect(read_only=True) as con:
        result = con.execute(
            f"SELECT * FROM {warehouse.qualify(table)} "
            f"WHERE {partition_col} = ?{order}",
            [partition_value],
        )
        header = [description[0] for description in result.description]
        rows = result.fetchall()
    return str(warehouse.write_partition(layer, name, partition_value, header, rows))
