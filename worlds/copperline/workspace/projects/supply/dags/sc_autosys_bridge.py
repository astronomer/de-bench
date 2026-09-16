"""Read the AutoSys run calendar and trigger what is due.

`legacy/autosys/copperline.jil` is the estate's own definition — thirty-four
boxes, exported from the scheduler — and `legacy/autosys/calendars/retail_2026.cal`
is the run calendar the boxes that name it obey. This DAG reads the calendar,
works out which boxes are due today, and starts them.

**Not everything runs every day.** The calendar names the days a box may start,
and the recon and distribution boxes carry it: finance do not read tie-outs at
the weekend, and a box that names the calendar does not start on a day the
calendar leaves out. `raw.market_calendar` is the warehouse's version of the
same fact and the two are maintained apart.

**A box that reports "nothing due today" exits non-zero.** That is a real
outcome and not a failure, which is why the runner is called with `check=False`
for the trigger and the exit code is read rather than raised on.

Wave 1's nine jobs were deleted from the JIL on the cutover and the deletes were
left in the file so the next wave can see what came out. They are not triggered
here and they are not in the calendar; they run in this deployment now.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import legacy_runner, warehouse, workspace_root
from include.lib.notify import notify

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

JIL = workspace_root() / "legacy" / "autosys" / "copperline.jil"
CALENDAR = workspace_root() / "legacy" / "autosys" / "calendars" / "retail_2026.cal"

#: The top box. Everything under it is conditioned on something above it, so
#: starting this one starts the night.
NIGHTLY_BOX = "cpl.nightly"

#: The exit code a box uses for "nothing due today". It is an outcome, not a
#: failure.
NOTHING_DUE = 4


def calendar_days() -> set[str]:
    """The dates `retail_2026.cal` names, as ISO strings.

    The file is the scheduler's own export: comment lines start with a `*`, and
    every other line is a date.
    """
    days = set()
    for line in CALENDAR.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("*"):
            days.add(line.split()[0])
    return days


def is_due(ds: str) -> bool:
    """Whether the calendar names today."""
    return ds in calendar_days()


def report_due(ds: str) -> str:
    """What the calendar says about today, for the log."""
    return f"{ds}: {'due' if is_due(ds) else 'not a calendar day'}"


def trigger_nightly(ds: str) -> int:
    """Start the nightly box, and take its exit code as an answer.

    `check=False`, because a box that reports nothing due exits non-zero and
    that is the right outcome for a day the calendar leaves out. Any other
    non-zero code is a failure and is raised here.
    """
    result = legacy_runner.run_step(
        JIL, NIGHTLY_BOX, variables={"BUSDATE": ds}, check=False,
    )
    if result.exit_code not in (0, NOTHING_DUE):
        raise legacy_runner.LegacyStepError(result)
    return result.exit_code


def report_boxes() -> list[str]:
    """The boxes the JIL still defines, for the migration's own bookkeeping.

    A `delete_job` line is a box wave 1 took out. Everything else still runs
    here, and the difference between the two lists is what the next wave has
    left to move.
    """
    lines = JIL.read_text(encoding="utf-8").splitlines()
    live = [line.split(":", 1)[1].strip().split()[0]
            for line in lines if line.startswith("insert_job:")]
    return sorted(live)


def check_jobs_wrote(ds: str) -> list[str]:
    """Which legacy jobs left a row in `ops.load_control` for the night.

    The control table is the estate's own record of itself. A job that started
    and wrote nothing is the shape a control break takes, and it is the only
    place a break shows before somebody notices a number.
    """
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT job_name, load_status FROM "
            f"{warehouse.qualify('ops.load_control')} "
            "WHERE business_date = ?::DATE ORDER BY job_name", [ds],
        ).fetchall()
    return [f"{job}: {status}" for job, status in rows]


with DAG(
    dag_id="sc_autosys_bridge",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "legacy", "autosys"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "AutoSys bridge"),
) as dag:
    due = PythonOperator(
        task_id="report_calendar_day", python_callable=report_due,
        op_kwargs={"ds": "{{ ds }}"})
    boxes = PythonOperator(
        task_id="report_boxes", python_callable=report_boxes)
    trigger = PythonOperator(
        task_id="trigger_nightly", python_callable=trigger_nightly,
        op_kwargs={"ds": "{{ ds }}"})
    wrote = PythonOperator(
        task_id="check_jobs_wrote", python_callable=check_jobs_wrote,
        op_kwargs={"ds": "{{ ds }}"})

    due >> boxes >> trigger >> wrote
