"""The orchestrator: base draw, then planting, then the refuse-to-write pass.

Build order inside one run:

1. the simulated upstream (`sim_*` schemas), module by module
2. the extracts (`raw.*`, `ops.*`, and the landing tree), module by module
3. the planting pass over planted.yaml
4. the invariant pass — any failure deletes the database and the landing
   tree and exits non-zero, so a bad world never reaches an agent
5. the sim schemas are dropped and the surviving tables are copied into a
   fresh file: DROP reclaims blocks but never returns them, and a full
   build otherwise ships gigabytes of dead upstream under a few hundred MB
   of extracts. The compact copy is also what the agent sees — extracts,
   never the fiction, not even as unreachable bytes.

The last extract module writes a SECOND file, `northwave.duckdb`, beside the
warehouse: the frozen `nwv` namespace the world attaches read-only. The
compaction swaps the main file alone, so that module writes its file
directly, and the failure path here deletes both.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

from . import config, extracts, invariants, planted_render, planting, upstream


def run(timeline_path: str, planted_path: str, db_path: str, landing_dir: str,
        profile: str, render_planted: str | None = None) -> None:
    cfg = config.load_timeline(timeline_path)
    planted = config.load_planted(planted_path)

    db = Path(db_path)
    landing = Path(landing_dir)
    db.parent.mkdir(parents=True, exist_ok=True)
    if db.exists():
        db.unlink()
    shutil.rmtree(landing, ignore_errors=True)
    landing.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db))
    ctx = config.Context(con=con, cfg=cfg, profile=profile, landing=landing)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        _build_days(ctx)
        for module in upstream.ORDER:
            module.build(ctx)
        for module in extracts.ORDER:
            module.build(ctx)
        planting.apply(ctx, planted)

        failures = invariants.run(ctx)
        if failures:
            raise SystemExit(
                "the invariant pass refuses to write this world:\n  - "
                + "\n  - ".join(failures)
            )

        for (schema,) in con.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name LIKE 'sim_%' OR schema_name = '_util'"
        ).fetchall():
            con.execute(f'DROP SCHEMA "{schema}" CASCADE')
        con.execute("CHECKPOINT")
        _compact(con, db)
    except BaseException:
        con.close()
        db.unlink(missing_ok=True)
        Path(f"{db}.compact").unlink(missing_ok=True)
        # The frozen nwv copy is a second file beside the warehouse, so a
        # world that refuses to be written has to take it away too.
        extracts.northwave_frozen.sibling_of(db).unlink(missing_ok=True)
        shutil.rmtree(landing, ignore_errors=True)
        raise
    con.close()
    Path(f"{db}.compact").replace(db)

    if render_planted:
        planted_render.write(render_planted, cfg, planted)


def _compact(con, db: Path) -> None:
    """Copy the surviving schemas into a fresh file beside the target. The
    caller swaps it over the original after closing — the connection still
    holds the old file, so the swap cannot happen here."""
    compact = f"{db}.compact"
    Path(compact).unlink(missing_ok=True)
    source = con.execute("SELECT current_database()").fetchone()[0]
    con.execute(f"ATTACH '{compact}' AS compact_target")
    con.execute(f'COPY FROM DATABASE "{source}" TO compact_target')
    con.execute("DETACH compact_target")


def _build_days(ctx: config.Context) -> None:
    """The shared day series — one computation of each day's order volume,
    which sales mints from and freight derives references from. The logic
    lives in upstream._util.ensure_days; hoisted here so it exists before any
    module runs regardless of ORDER."""
    from .upstream import _util

    _util.ensure_days(ctx)
