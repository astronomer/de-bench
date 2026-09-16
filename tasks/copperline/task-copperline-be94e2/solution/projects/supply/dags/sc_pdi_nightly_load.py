"""The legacy Pentaho nightly load, driven step by step over SSH.

Thirteen of the twenty-two nightly Pentaho jobs still run. Wave 1 moved nine of
them to Airflow on the cutover; the thirteen below have not moved and this DAG
is what runs them. `legacy/pdi/nightly_load.kjb` is the job and
`legacy/pdi/README.txt` is what its author wrote about it in 2021. Both are
still true.

**One step per task.** A step that fails retries alone and the graph shows which
step it was. That is the team rule and it is the whole reason
`include.lib.legacy_runner` exists.

**The variable block flows forward and nothing here invents one.**
`set_constants` fixes the staging schema and the watermark column.
`resolve_load_control` reads `ops.load_control` once, for the night, and exports
four variables. Each load step consumes them. A step that resolves its own
watermark loads a window a sibling step has already loaded, and nothing about
that is visible until the numbers are double — the note on the job graph says
so in the author's own words.

**The site settings are handed to the job, not added to the block.**
`set_constants` reads the watermark column from `kettle.properties` on the PDI
box. AutoSys used to give the job that file's settings through
`/opt/cpl/etc/nightly.profile`; this deployment does not, and since 2026-04-06
the step has come back without a watermark column, which is what stopped the
loads. `SITE_SETTINGS` passes the setting to the step that reads it, the same
way the job is handed `JOB_NAME` and `BUSINESS_DATE`. The box is frozen, so the
setting lives here now. `ops/incidents/2026-06-15-legacy-nightly-failing.md` is
the note.

**`history_complete` brackets the job, not the step.** It runs once, after the
last load step. Running it after each step marks the history complete while most
of it is still missing. It is `sc_pdi_history_complete` and it is a DAG of its
own so that the bracket can be closed by hand after a repair.

Produces whatever the thirteen jobs produce, which is supply-chain reporting on
the old schema. Nothing in `raw` depends on it.

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
    # Three, because the old box drops an SSH session about once a week and a
    # dropped session is not a failed step.
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

LEGACY = workspace_root() / "legacy" / "pdi"

#: The job's steps, in the order `nightly_load.kjb` hops them. The two at the
#: front set the context; the four in the middle consume it. `history_complete`
#: is not here — it brackets the job and it is its own DAG.
STEPS = (
    ("set_constants", "trn/set_constants.ktr"),
    ("resolve_load_control", "trn/resolve_load_control.ktr"),
    ("load_inventory", "trn/load_inventory.ktr"),
    ("load_movements", "trn/load_movements.ktr"),
    ("load_carrier_scans", "trn/load_carrier_scans.ktr"),
    ("load_supplier_master", "trn/load_supplier_master.ktr"),
)

#: The variables `resolve_load_control` is expected to export. A night that
#: comes back short of one of them has a control break, and the load steps then
#: have nothing to filter on.
REQUIRED_VARIABLES = ("LOAD_ID", "STREAM_NAME", "LOAD_FROM_COLUMN",
                      "LOAD_FROM")

#: Site settings, by the step that reads them. `set_constants` takes the
#: watermark column from `${COPPERLINE_WATERMARK_COL}`, which used to reach it
#: from `kettle.properties` on the PDI box. `loaded_at` is the column every
#: control row up to 2026-04-05 names — `ops.load_control.watermark_column` —
#: and it is the mark the load steps have always cut from.
#:
#: One step, not all of them, and a parameter rather than a block entry: this
#: is the job being called with its settings, the way `kitchen.sh` is called in
#: `legacy/autosys/copperline.jil`. Nothing here adds a variable to the block a
#: step hands the next one.
SITE_SETTINGS = {
    "set_constants": {"COPPERLINE_WATERMARK_COL": "loaded_at"},
}


def run_step(step: str, artifact: str, ds: str, ti=None) -> dict:
    """Run one step, with the block the step before it produced.

    The block is passed through exactly as it came back. Adding a variable the
    job did not set makes this run disagree with the nightly one, and defaulting
    a missing one hides the night the job stopped setting it. The site settings
    are not part of the block: they are parameters the step is called with, like
    `JOB_NAME` and `BUSINESS_DATE`.
    """
    upstream = ti.xcom_pull(task_ids=_previous(step)) if _previous(step) else {}
    result = legacy_runner.run_step(
        LEGACY / artifact,
        step,
        variables={**(upstream or {}), "JOB_NAME": "nightly_load",
                   "BUSINESS_DATE": ds, **SITE_SETTINGS.get(step, {})},
    )
    return result.variables


def check_control_context(ti=None) -> list[str]:
    """The control step exported every variable the load steps read.

    A night that resolves a context without a watermark leaves the loads with
    nothing to filter on, and they load everything or nothing depending on the
    step. Neither is loud.
    """
    variables = ti.xcom_pull(task_ids="resolve_load_control") or {}
    missing = [name for name in REQUIRED_VARIABLES if not variables.get(name)]
    if missing:
        raise ValueError(
            "resolve_load_control set no "
            f"{', '.join(missing)}. The load steps have nothing to filter on; "
            "do not run them with a default."
        )
    return sorted(variables)


def record_night(ds: str) -> int:
    """The night's own row, from `ops.load_control`.

    The control table is the legacy estate's record of itself and it is
    read-only here: this reports what the night wrote, it does not write it.
    """
    with warehouse.connect(read_only=True) as con:
        return int(con.execute(
            f"SELECT count(*) FROM {warehouse.qualify('ops.load_control')} "
            "WHERE business_date = ?::DATE AND load_status = 'complete'",
            [ds],
        ).fetchone()[0])


def _previous(step: str) -> str | None:
    """The step before this one, or None for the first."""
    names = [name for name, _ in STEPS]
    index = names.index(step)
    return names[index - 1] if index else None


with DAG(
    dag_id="sc_pdi_nightly_load",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "legacy", "pdi"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "legacy nightly load"),
) as dag:
    tasks = []
    for _step, _artifact in STEPS:
        _task = PythonOperator(
            task_id=_step,
            python_callable=run_step,
            op_kwargs={"step": _step, "artifact": _artifact, "ds": "{{ ds }}"},
        )
        if tasks:
            tasks[-1] >> _task
        tasks.append(_task)

    context_check = PythonOperator(
        task_id="check_control_context", python_callable=check_control_context)
    night = PythonOperator(
        task_id="record_night", python_callable=record_night,
        op_kwargs={"ds": "{{ ds }}"})

    # The check sits beside the load steps rather than between them: it reads
    # what the control step exported, and it must not become another thing the
    # loads wait on.
    tasks[1] >> context_check
    tasks[-1] >> night
