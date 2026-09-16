"""The SSIS supplier-master package, run one executable at a time.

`legacy/ssis/SupplierMaster.dtsx` extracts the supplier master out of the ERP and
writes a pipe-delimited drop to the interface share. It was written in 2016 and
it is one of the nine jobs wave 1 moved to Airflow. The package still runs on
the old box; what moved is what starts it.

**The graph is the package's own control flow.** One task per executable, in the
order the precedence constraints hop them, so a person reading this DAG and a
person reading the package in BIDS see the same shape:

- `truncate_staging` runs first, and two things hang off it on success.
- `archive_previous_drop` is **disabled in the package** — turned off in 2019
  when the archive share filled up, and left in place. It is here, and it is
  skipped, because a task that is not in the graph is a task nobody knows is
  off.
- `extract_suppliers` is the data flow. Its success path and its failure path
  are separate constraints and both are here.
- `notify_ops` is an OR of two completions, not an AND: it mails whichever way
  the run went. `one_done` is the rule that says that.

**A drop that reads nothing still finishes green.** The share is mounted at
`CPL_IFACE_IN`, and when the share is not there the flat-file source reads
nothing and the package succeeds. `legacy/pdi/README.txt` records that as a
low-priority TODO from 2021; the Pentaho side reads the same file.

Produces the supplier drop on the interface share, which
`sc_pdi_nightly_load`'s `load_supplier_master` step consumes.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk import DAG

from include.lib import legacy_runner, workspace_root
from include.lib.notify import notify
from projects.supply.lib import paths

DEFAULT_ARGS = {
    "owner": "supply",
    # Three, because the old box drops an SSH session about once a week.
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

PACKAGE = workspace_root() / "legacy" / "ssis" / "SupplierMaster.dtsx"

#: The executables the package disables. They stay in the graph and they skip,
#: because an executable that vanishes from the graph is one nobody remembers is
#: off.
DISABLED = ("FSYS Archive Previous Drop",)


def run_executable(executable: str, ds: str) -> dict:
    """Run one executable of the package, by its object name.

    The package's own name is passed through as the step so the log on the old
    box and the task id here read the same.
    """
    if executable in DISABLED:
        raise AirflowSkipException(
            f"{executable} is disabled in the package and has been since 2019"
        )
    result = legacy_runner.run_step(
        PACKAGE, executable, variables={"BusinessDate": ds},
    )
    return result.variables


def log_failure(ds: str) -> str:
    """The package's failure path: log the run as failed and carry on to the
    notification, which is what the precedence constraint does."""
    return f"SupplierMaster FAILED for {ds}"


def notify_ops(ds: str) -> str:
    """The package mails ops whichever way the run went.

    Both constraints into it are on completion and they are OR-ed, so this runs
    when either path has finished. It is not an all-parents rule and it is not a
    cleanup task: a failed extract still reaches here and still says so.
    """
    return f"SupplierMaster finished for {ds}"


def check_drop_written(ds: str) -> int:
    """The size of the drop the package left on the share.

    Reported, not failed. The file is not dated and each night overwrites it, so
    a night that wrote nothing leaves yesterday's file in place and its size
    tells you nothing on its own. What the number is for is the trend somebody
    looks at when the supplier counts move.
    """
    path = paths.supplier_drop()
    return path.stat().st_size if path.exists() else 0


with DAG(
    dag_id="sc_ssis_supplier_master",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "legacy", "ssis"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "supplier master package"),
) as dag:
    truncate = PythonOperator(
        task_id="truncate_staging", python_callable=run_executable,
        op_kwargs={"executable": "SQL Truncate Staging", "ds": "{{ ds }}"})

    archive = PythonOperator(
        task_id="archive_previous_drop", python_callable=run_executable,
        op_kwargs={"executable": "FSYS Archive Previous Drop", "ds": "{{ ds }}"})

    extract = PythonOperator(
        task_id="extract_suppliers", python_callable=run_executable,
        op_kwargs={"executable": "DFT Extract Suppliers", "ds": "{{ ds }}"})

    log_rows = PythonOperator(
        task_id="log_row_count", python_callable=run_executable,
        op_kwargs={"executable": "SQL Log Row Count", "ds": "{{ ds }}"})

    log_failed = PythonOperator(
        task_id="log_failure", python_callable=log_failure,
        op_kwargs={"ds": "{{ ds }}"},
        trigger_rule="one_failed")

    ops_mail = PythonOperator(
        task_id="notify_ops", python_callable=notify_ops,
        op_kwargs={"ds": "{{ ds }}"},
        trigger_rule="one_done")

    drop = PythonOperator(
        task_id="check_drop_written", python_callable=check_drop_written,
        op_kwargs={"ds": "{{ ds }}"})

    truncate >> [archive, extract]
    extract >> [log_rows, log_failed]
    [log_rows, log_failed] >> ops_mail >> drop
