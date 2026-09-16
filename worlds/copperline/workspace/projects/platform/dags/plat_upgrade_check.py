"""The Monday pre-flight for a package upgrade.

Runs `tools/check_upgrade.py` over the tree and records what it said. It is the
check `docs/runbooks/upgrade.md` points at, and it runs weekly so that the
answer is never older than a week when somebody needs it.

The checker walks the DAG folders for imports and compares them against the
symbols removed at the pinned versions. That is what it does; the runbook says
what an upgrade actually needs, and the two are not the same length of list.

Owned by data-platform.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from include.lib import warehouse, workspace_root
from include.lib.notify import notify
from include.lib.pipeline import lake_task

CHECKER = workspace_root() / "tools" / "check_upgrade.py"

#: Where the week's output is kept for the parse step to read.
REPORT = workspace_root() / "ops" / "upgrade" / "check_upgrade.txt"

#: Where the weekly answer is kept, so a change in it is visible.
TABLE = "ops.upgrade_check"

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("platform", "the weekly upgrade check did not run"),
}


@dag(
    dag_id="plat_upgrade_check",
    schedule="0 7 * * 1",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "upgrade", "ops"],
    doc_md=__doc__,
)
def plat_upgrade_check():
    run_checker = BashOperator(
        task_id="run_checker",
        # The whole output is kept, not the last line: the summary is one line
        # and the findings are the rest, and it is the rest that matters.
        bash_command=(f"mkdir -p {REPORT.parent} && "
                      f"python {CHECKER} --tree {workspace_root()} | tee {REPORT}"),
    )

    @lake_task
    def parse() -> dict:
        """Turn the checker's lines into a count and a list.

        The checker prints one line per finding and a closing line when it has
        none. Anything it does not print is not in this answer.
        """
        lines = [line.strip() for line in
                 REPORT.read_text(encoding="utf-8").splitlines() if line.strip()]
        findings = [line for line in lines if not line.startswith("no blocking issues")]
        return {"findings": findings, "count": len(findings)}

    @lake_task
    def record(result: dict, target_ds: str) -> int:
        """Keep the week's answer beside the ones before it."""
        findings = result["findings"] or [None]
        rows = [{"ds": target_ds, "finding": finding} for finding in findings]
        return warehouse.delete_insert(
            TABLE, "ds", target_ds, rows, columns=["ds", "finding"]
        )

    parsed = parse()
    run_checker >> parsed
    record(parsed, target_ds="{{ ds }}")


plat_upgrade_check()
