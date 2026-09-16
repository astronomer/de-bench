"""Copperline's house library.

Every team imports this package. The data-platform team owns it (P. Vance,
H. Oyelaran) and nothing outside it may open the warehouse, build a path or
write a partition.

    calendar.py        the 4-5-4 fiscal calendar, the market calendar, today()
    warehouse.py       connect(), delete_insert(), write_partition()
    watermark.py       since(), advance()
    pipeline.py        @lake_task, Reject, the run manifest
    loaders.py         CsvToWarehouseOperator, JsonApiToWarehouseOperator
    contracts.py       loads contracts/<mart>.yml and enforces it
    legacy_runner.py   runs one step of a PDI job, an SSIS package or a JIL box
    blueprint/         the factory that renders projects/*/dags/*.dag.yaml

Two rules hold across all of it.

**Nothing here reads the wall clock.** A run is told which dates it owns, by
the interval in its context or by `calendar.today()`, which reads the
`WORLD_TODAY` environment variable the platform exports. `datetime.now()` in
this tree is a defect, not a shortcut.

**Nothing here does work at import.** These modules are imported on every DAG
parse. They open no file, no warehouse and no socket until something calls
them.

The four helpers below say where the tree is. They resolve from this file, so
they give the same answer whatever a task's working directory happens to be.
"""

from __future__ import annotations

from pathlib import Path

__all__ = ["workspace_root", "data_dir", "landing_dir", "contracts_dir"]


def workspace_root() -> Path:
    """The repository root — the directory that holds `include/`, `landing/`,
    `projects/`, `dbt/` and `contracts/`."""
    return Path(__file__).resolve().parents[2]


def data_dir() -> Path:
    """`include/data/` — the warehouse file, the published partitions and the
    run manifests. Written at run time; nothing in it is committed."""
    return workspace_root() / "include" / "data"


def landing_dir(source: str | None = None) -> Path:
    """`landing/`, or one source's subtree of it.

    The landing tree is dated and hive-style. Most sources land one directory
    a day, `landing/<source>/dt=<ds>/<file>.csv`; the clickstream and the
    Meridian feed add an hour level under that; the carrier feeds are keyed by
    carrier rather than by date. `docs/retention-policy.md` says how long each
    one keeps its files.
    """
    root = workspace_root() / "landing"
    return root / source if source else root


def contracts_dir() -> Path:
    """`contracts/` — one `<mart>.yml` per published mart, and the prose
    contract beside it. `contracts.py` reads them."""
    return workspace_root() / "contracts"
