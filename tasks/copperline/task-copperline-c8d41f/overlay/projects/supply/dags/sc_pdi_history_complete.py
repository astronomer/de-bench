"""Close the legacy history bracket for the night.

`legacy/pdi/trn/history_complete.ktr` brackets the **job**, not the step. It runs
once, after the last load step, and it marks the night's history complete for
the whole job. Running it after each step marks the history complete while most
of it is still missing, and the next night then starts from a mark that stands
for a night that never finished.

It is a DAG of its own rather than the last task of `sc_pdi_nightly_load` for
one reason: after a repair, somebody has to close the bracket by hand for a
night whose loads were re-run outside the schedule. A separate DAG can be
triggered for that night; a last task cannot, without re-running the loads.

It runs at 02:00, after the 01:00 load. Nothing wires the two together, which is
how the estate has always worked.

Wave 2 preparation, April. The guard used to close over whatever rows the night
happened to write, which says nothing about a job that never started. It now
asks the scheduler's own export what the night owed:
`legacy/autosys/copperline.jil` is the estate's definition of itself, and a box
under `cpl.nightly.load` is one nightly job.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import re

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import legacy_runner, warehouse, workspace_root
from include.lib.notify import notify

DEFAULT_ARGS = {
    "owner": "supply",
    # Three, because the old box drops an SSH session about once a week.
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

ARTIFACT = workspace_root() / "legacy" / "pdi" / "trn" / "history_complete.ktr"

#: The scheduler's own export of the estate.
JIL = workspace_root() / "legacy" / "autosys" / "copperline.jil"

#: The box every nightly load job sits in.
LOAD_BOX = "cpl.nightly.load"

#: The night this deployment took the estate over. Before it, wave 1's nine
#: jobs were still on the old scheduler and still owed a row.
CUTOVER = "2026-01-05"


def load_boxes() -> tuple[list[str], list[str]]:
    """The nightly load jobs the export names: the ones still on the old
    scheduler, and the ones wave 1 took out.

    A box under `cpl.nightly.load` is one nightly job, and its name without the
    `cpl.` prefix is the name it writes to `ops.load_control` under. Wave 1's
    nine are the `delete_job` lines at the top of the same file.
    """
    text = JIL.read_text(encoding="utf-8")
    migrated = [
        name.removeprefix("cpl.")
        for name in re.findall(r"(?m)^delete_job:\s*(\S+)\s*$", text)
    ]
    surviving = []
    for block in re.split(r"(?m)^insert_job:", text)[1:]:
        if re.search(rf"(?m)^box_name:\s*{re.escape(LOAD_BOX)}\s*$", block):
            surviving.append(block.split()[0].removeprefix("cpl."))
    return surviving, migrated


def due_jobs(ds: str) -> list[str]:
    """The legacy jobs the night owed a row for."""
    surviving, migrated = load_boxes()
    return sorted(surviving if ds >= CUTOVER else surviving + migrated)


def check_loads_finished(ds: str) -> int:
    """Every job the night owed reached `complete` before the bracket closes.

    The control table is the estate's own record and it is read-only here. A
    night with a failed load is a night whose history is not complete, and
    saying so is the point of the bracket.
    """
    with warehouse.connect(read_only=True) as con:
        done = {
            row[0]
            for row in con.execute(
                "SELECT job_name FROM "
                f"{warehouse.qualify('ops.load_control')} "
                "WHERE business_date = ?::DATE AND load_status = 'complete'",
                [ds],
            ).fetchall()
        }
    short = [job for job in due_jobs(ds) if job not in done]
    if short:
        raise ValueError(
            f"{ds}: {len(short)} legacy jobs did not complete "
            f"({', '.join(short[:5])}). The bracket does not close over a "
            "night that did not finish."
        )
    return len(done)


def close_bracket(ds: str) -> dict:
    """Run `history_complete` once, for the job and the night."""
    result = legacy_runner.run_step(
        ARTIFACT,
        "history_complete",
        variables={"JOB_NAME": "nightly_load", "BUSINESS_DATE": ds},
    )
    return result.variables


with DAG(
    dag_id="sc_pdi_history_complete",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "legacy", "pdi"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "legacy history bracket"),
) as dag:
    loads = PythonOperator(
        task_id="check_loads_finished", python_callable=check_loads_finished,
        op_kwargs={"ds": "{{ ds }}"})
    bracket = PythonOperator(
        task_id="history_complete", python_callable=close_bracket,
        op_kwargs={"ds": "{{ ds }}"})

    loads >> bracket
