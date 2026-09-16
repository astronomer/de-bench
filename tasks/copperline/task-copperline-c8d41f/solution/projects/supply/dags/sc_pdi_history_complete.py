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

**What a night owed is not what a night wrote.** The guard asks the scheduler's
own export — `legacy/autosys/copperline.jil` — for the roster, and then asks
`ops.load_control` whether the roster completed. Taking the roster from the
control table instead would close every night that wrote nothing, which is the
one night the bracket exists to refuse.

**The roster is not the same list every night.** A box under `cpl.nightly.load`
is one nightly job, but a box may carry its own `days_of_week`, and one of the
thirteen does: `cpl.nightly_store_targets` runs `su` and no other day. The
control table agrees — it holds that job on Sunday business dates and on no
others — so a roster of thirteen is right one night in seven and refuses the
other six.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import datetime as dt
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

#: How AutoSys writes the days a box may start, Sunday first. A box with no
#: `days_of_week` of its own runs whenever its parent box runs, and the nightly
#: window runs `all`.
DAY_CODES = ("su", "mo", "tu", "we", "th", "fr", "sa")


def load_boxes() -> tuple[dict[str, str], list[str]]:
    """The nightly load jobs the export names.

    The first return is the jobs still on the old scheduler, each against the
    `days_of_week` its box carries (an empty string where the box carries
    none). The second is the nine wave 1 took out, which the export keeps as
    `delete_job` lines and holds no attributes for.

    A box under `cpl.nightly.load` is one nightly job, and its name without the
    `cpl.` prefix is the name it writes to `ops.load_control` under.
    """
    text = JIL.read_text(encoding="utf-8")
    migrated = [
        name.removeprefix("cpl.")
        for name in re.findall(r"(?m)^delete_job:\s*(\S+)\s*$", text)
    ]
    surviving: dict[str, str] = {}
    for block in re.split(r"(?m)^insert_job:", text)[1:]:
        if not re.search(rf"(?m)^box_name:\s*{re.escape(LOAD_BOX)}\s*$", block):
            continue
        days = re.search(r"(?m)^days_of_week:\s*(.*?)\s*$", block)
        surviving[block.split()[0].removeprefix("cpl.")] = days.group(1) if days else ""
    return surviving, migrated


def runs_on(days: str, night: dt.date) -> bool:
    """Whether a box carrying these `days_of_week` runs for that business date.

    Nothing and `all` both mean every night. The only load box that names its
    own days also carries `date_conditions: 1`, which is what makes them count.
    """
    if not days or days == "all":
        return True
    named = {code.strip() for code in days.replace(",", " ").split()}
    return DAY_CODES[(night.weekday() + 1) % 7] in named


def due_jobs(ds: str) -> list[str]:
    """The legacy jobs the night owed a row for.

    Answers from the export for any night, including one `ops.load_control` has
    no row for: a night that wrote nothing owed as much as any other, and that
    is the night the bracket has to refuse.
    """
    surviving, migrated = load_boxes()
    night = dt.date.fromisoformat(ds)
    owed = [job for job, days in surviving.items() if runs_on(days, night)]
    if ds < CUTOVER:
        # The export holds no attributes for a job it no longer defines, so
        # wave 1's nine come back whole. This deployment closes brackets for
        # nights it ran, and it has run since the cutover.
        owed += migrated
    return sorted(owed)


def check_loads_finished(ds: str) -> int:
    """Every job the night owed reached `complete` before the bracket closes.

    The control table is the estate's own record and it is read-only here. A
    night with a failed load is a night whose history is not complete, and so
    is a night that is short a job it owed — saying so is the point of the
    bracket.
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
