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
    # Three, because the old box drops an SSH session about once a week.
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

ARTIFACT = workspace_root() / "legacy" / "pdi" / "trn" / "history_complete.ktr"


def check_loads_finished(ds: str) -> int:
    """The night's load steps all reached `complete` before the bracket closes.

    The control table is the estate's own record and it is read-only here. A
    night with a failed load is a night whose history is not complete, and
    saying so is the point of the bracket.
    """
    with warehouse.connect(read_only=True) as con:
        failed = con.execute(
            "SELECT job_name FROM "
            f"{warehouse.qualify('ops.load_control')} "
            "WHERE business_date = ?::DATE AND load_status <> 'complete' "
            "ORDER BY job_name",
            [ds],
        ).fetchall()
    if failed:
        names = ", ".join(str(row[0]) for row in failed[:5])
        raise ValueError(
            f"{ds}: {len(failed)} legacy jobs did not complete ({names}). "
            "The bracket does not close over a night that did not finish."
        )
    return 0


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
