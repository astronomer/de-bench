"""Move landing files out of the live tree when their retention runs out.

`docs/retention-policy.md` is the rule and this DAG is where it happens.

- RET-1, a vendor file — the processors, the carriers, the ad platforms —
  keeps 90 days live.
- RET-2, one of ours — the OMS extracts, the POS batches, the clickstream, the
  WMS files, the PIM feed, the parquet export — keeps 400 days.
- RET-5, the ledger and anything under legal hold, is exempt and keeps seven
  years. This DAG never touches it.

It moves; it does not delete. The archive copy becomes the only copy, reading
it is a restore job rather than a query, and there is no second archive. That
is why the move is recorded per file: when something is not in the live tree
and not in the archive, the record is what says whether it ever existed.

The cutoff is measured from `include.lib.calendar.today()`, the business date
the platform exports. A retention rule is about how old a file is now, which is
one of the few questions in this tree that is genuinely about now.

Owned by data-platform.
"""

from __future__ import annotations

import datetime as dt
import shutil

import pendulum
from airflow.sdk import dag

from include.lib import calendar as cal
from include.lib import landing_dir, warehouse, workspace_root
from include.lib.notify import notify
from include.lib.pipeline import lake_task

ARCHIVE = workspace_root() / "archive" / "landing"

#: What was moved, and when. One row per file.
TABLE = "ops.retention_log"

#: RET-1: a feed a third party sends us.
VENDOR_SOURCES = ("meridian", "halcyon", "carriers", "adplatforms", "email",
                  "comp_prices")

#: RET-2: a feed we produce ourselves.
OWN_SOURCES = ("oms", "pos", "clickstream", "wms", "pim", "orders_parquet",
               "crm", "loyalty")

#: RET-5. Named here so that a change to the lists above cannot reach them.
EXEMPT_SOURCES = ("finance", "ledger", "legal_hold")

VENDOR_DAYS = 90
OWN_DAYS = 400

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify("platform", "landing retention did not run"),
}


@dag(
    dag_id="plat_partition_retention",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "retention", "landing"],
    doc_md=__doc__,
)
def plat_partition_retention():
    @lake_task
    def plan() -> dict[str, list[str]]:
        """Which dated directories are past their class's retention.

        The landing tree is `landing/<source>/dt=<ds>/`, so the cutoff is a
        string comparison against the directory name and nothing has to be
        opened to decide.
        """
        today = cal.today()
        vendor_cutoff = (today - dt.timedelta(days=VENDOR_DAYS)).isoformat()
        own_cutoff = (today - dt.timedelta(days=OWN_DAYS)).isoformat()
        due: dict[str, list[str]] = {"vendor": [], "own": []}
        for group, sources, cutoff in (
            ("vendor", VENDOR_SOURCES, vendor_cutoff),
            ("own", OWN_SOURCES, own_cutoff),
        ):
            for source in sources:
                root = landing_dir(source)
                if not root.exists():
                    continue
                for day in sorted(root.glob("dt=*")):
                    if day.name[len("dt="):] < cutoff:
                        due[group].append(str(day.relative_to(workspace_root())))
        return due

    @lake_task
    def refuse_exempt(due: dict[str, list[str]]) -> dict[str, list[str]]:
        """Drop anything under an exempt source before a file is moved.

        RET-5 is a floor, not a preference, and the cheapest place to enforce
        it is before the move rather than after. A path that reaches here from
        an exempt source is a bug in `plan`, and it is worth failing over.
        """
        offending = [
            path for paths in due.values() for path in paths
            if any(f"landing/{source}/" in path for source in EXEMPT_SOURCES)
        ]
        if offending:
            raise RuntimeError(
                "retention reached an exempt source: " + ", ".join(sorted(offending))
            )
        return due

    @lake_task
    def archive_vendor(due: dict[str, list[str]]) -> list[str]:
        """Move the vendor days past 90. RET-1."""
        return _move(due["vendor"])

    @lake_task
    def archive_own(due: dict[str, list[str]]) -> list[str]:
        """Move our own days past 400. RET-2.

        Four hundred days is thirteen months and a bit, so a full year is
        always live along with the period it is compared against.
        """
        return _move(due["own"])

    @lake_task
    def record(moved: list[list[str]], target_ds: str) -> int:
        """One row per moved directory, in `ops.retention_log`."""
        rows = [{"ds": target_ds, "path": path, "kind": kind}
                for kind, paths in zip(("vendor", "own"), moved)
                for path in paths]
        return warehouse.delete_insert(
            TABLE, "ds", target_ds, rows, columns=["ds", "path", "kind"]
        )

    checked = refuse_exempt(plan())
    record([archive_vendor(checked), archive_own(checked)], target_ds="{{ ds }}")


def _move(paths: list[str]) -> list[str]:
    """Move each directory under `archive/landing/`, keeping its layout.

    Moved rather than copied and deleted: a move within the tree is atomic, so
    a run that dies halfway leaves a directory in one place or the other and
    never in neither.
    """
    moved = []
    for path in paths:
        source = workspace_root() / path
        if not source.exists():
            continue
        target = ARCHIVE / path[len("landing/"):]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(target))
        moved.append(path)
    return moved


plat_partition_retention()
