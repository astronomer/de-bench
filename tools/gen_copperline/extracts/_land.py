"""Writing the dated landing tree.

Every source in spec chapter 03 section 2 lands files under a hive-style
path, and each one states the exact leaf file name — `changes.csv`,
`beacon.csv`, `part-000.parquet`. DuckDB writes hive partitions in one
statement, which is the only way a landing tree of a few thousand files is
affordable inside trial setup, but it always appends a file index to the
name it is given. So the COPY runs once and the leaves are renamed after
it, and the rename asserts one file per partition rather than hoping.

**The landing window.** `docs/retention-policy.md` gives every source a live
window: 90 days for a vendor file (RET-1), 400 days for one of Copperline's
own (RET-2). The generator honours those windows where the file count is
affordable and says so where it is not. `raw.*` always holds the whole
history; the landing tree holds what the ingest DAGs still read.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from ..config import Context

# The two windows docs/retention-policy.md states.
VENDOR_DAYS = 90       # RET-1
OWN_DAYS = 400         # RET-2


def window_start(ctx: Context, days: int) -> dt.date:
    """The first date whose files are still live, counting back from the end
    of the fixture range."""
    return ctx.end - dt.timedelta(days=days - 1)


def write_hive(ctx: Context, relation: str, dest: Path, fmt: str,
               partition_by: tuple[str, ...], leaf: str,
               options: str = "") -> int:
    """COPY a relation into `dest` as a hive-partitioned tree, then give every
    leaf file the name the source's contract states.

    `relation` is a SELECT. `leaf` is the file name without its extension.
    Returns the number of files written.
    """
    dest.mkdir(parents=True, exist_ok=True)
    suffix = "parquet" if fmt.upper() == "PARQUET" else "csv"
    extra = f", {options}" if options else ""
    # One writer, so one file per partition. A parallel partitioned write
    # flushes once per thread, and six `part-00*.parquet` in an hour that a
    # collector delivers once is not what the contract describes.
    threads = ctx.sql("SELECT current_setting('threads')").fetchone()[0]
    ctx.sql("SET threads = 1")
    try:
        ctx.sql(
            f"COPY ({relation}) TO '{dest.as_posix()}' "
            f"(FORMAT {fmt}, PARTITION_BY ({', '.join(partition_by)}), "
            f"FILENAME_PATTERN '{leaf}_{{i}}', OVERWRITE_OR_IGNORE{extra})"
        )
    finally:
        ctx.sql(f"SET threads = {int(threads)}")
    written = 0
    for path in sorted(dest.rglob(f"{leaf}_*.{suffix}")):
        # More than one file in a partition would mean two writers split it,
        # and the contract names a single file. Fail rather than lose rows.
        siblings = list(path.parent.glob(f"{leaf}_*.{suffix}"))
        assert len(siblings) == 1, f"{path.parent} holds {len(siblings)} files"
        path.rename(path.with_name(f"{leaf}.{suffix}"))
        written += 1
    return written
