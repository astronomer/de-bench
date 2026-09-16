"""Pick up the analyst workbooks and hand each one to the team that owns it.

Analysts drop spreadsheets into `landing/<team>/`. They are exports from a
workbook somebody keeps by hand, they arrive when they arrive, and at least one
of them is the source of a figure that reaches the ledger. This DAG finds them,
records what arrived, and stages each file where the owning team's DAG expects
to read it.

It does not parse any of them. A workbook's shape is the owning team's problem
— merged headers, a total row at the bottom, a column that changed meaning at
the fiscal-year boundary — and a platform DAG that tried to understand one
would be wrong about it within a month.

A file for a team that is not in the `workbook_inbox_teams` variable is
quarantined rather than dropped, because the usual cause is a folder named
after a person.

Owned by data-platform.
"""

from __future__ import annotations

import shutil

import pendulum
from airflow.sdk import dag

from include.lib import landing_dir, warehouse, workspace_root
from include.lib.notify import notify
from include.lib.pipeline import lake_task

#: Where a staged workbook goes, ready for its team.
STAGING = workspace_root() / "landing" / "_workbooks"

#: Where a file nobody claims goes.
QUARANTINE = workspace_root() / "landing" / "_quarantine"

#: One row per file seen, per day.
TABLE = "ops.workbook_inbox"

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("platform", "the workbook inbox did not run"),
}


@dag(
    dag_id="plat_workbook_inbox",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "landing", "workbooks"],
    doc_md=__doc__,
)
def plat_workbook_inbox():
    @lake_task
    def find_files() -> list[dict]:
        """Every CSV sitting under `landing/<team>/`, with its team and size.

        The size is here because a workbook that arrives at nought bytes is the
        failure that has happened most: the analyst's export ran while the
        spreadsheet was open, and the file is a header and nothing else.
        """
        teams = _declared_teams()
        found = []
        for team in sorted(teams):
            root = landing_dir(team)
            if not root.exists():
                continue
            for path in sorted(root.glob("*.csv")):
                found.append({
                    "team": team,
                    "name": path.name,
                    "path": str(path.relative_to(workspace_root())),
                    "bytes": path.stat().st_size,
                })
        return found

    @lake_task(map_index_template="{{ task.op_kwargs['workbook']['name'] }}")
    def stage(workbook: dict) -> dict:
        """Copy one workbook to `landing/_workbooks/<team>/<name>`.

        Copied rather than moved. The analyst's folder is the analyst's, and a
        file that vanished overnight is a support conversation nobody enjoys.
        The staged copy is what the owning team's DAG reads, so a re-export
        later in the day replaces the copy on the next run.
        """
        source = workspace_root() / workbook["path"]
        target = STAGING / workbook["team"] / workbook["name"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        return {**workbook, "staged_to": str(target.relative_to(workspace_root()))}

    @lake_task
    def quarantine_unclaimed() -> list[str]:
        """Move a file under a folder no team claims out of the way.

        It is moved, not deleted, and the move is what gets somebody to ask
        where their file went — which is the conversation that ends with the
        folder being named after a team.
        """
        teams = _declared_teams()
        moved = []
        root = workspace_root() / "landing"
        for folder in sorted(p for p in root.glob("*") if p.is_dir()):
            if folder.name in teams or folder.name.startswith("_"):
                continue
            for path in sorted(folder.glob("*.csv")):
                target = QUARANTINE / folder.name / path.name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(path), str(target))
                moved.append(str(target.relative_to(workspace_root())))
        return moved

    @lake_task
    def record(staged: list[dict], quarantined: list[str], target_ds: str) -> int:
        """One row per file, staged or quarantined, for the day."""
        rows = [
            {"ds": target_ds, "team": item["team"], "name": item["name"],
             "bytes": item["bytes"], "state": "staged"}
            for item in staged
        ] + [
            {"ds": target_ds, "team": path.split("/")[2], "name": path.split("/")[-1],
             "bytes": None, "state": "quarantined"}
            for path in quarantined
        ]
        return warehouse.delete_insert(
            TABLE, "ds", target_ds, rows,
            columns=["ds", "team", "name", "bytes", "state"],
        )

    staged = stage.expand(workbook=find_files())
    record(staged, quarantine_unclaimed(), target_ds="{{ ds }}")


def _declared_teams() -> set[str]:
    """The teams allowed an inbox, from the `workbook_inbox_teams` variable.

    Read when the task runs. Reading it at parse would put a variable lookup in
    front of every DAG parse in the deployment.
    """
    from airflow.sdk import Variable

    declared = Variable.get("workbook_inbox_teams", default="")
    return {name.strip() for name in str(declared).split(",") if name.strip()}


plat_workbook_inbox()
