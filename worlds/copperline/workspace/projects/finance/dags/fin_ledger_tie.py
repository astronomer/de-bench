"""Compare the published figures to the ledger and write `ops.tie_breaks`.

A published figure is not correct until this agrees with it. That is the
team's rule and §REV-9 is the policy behind it: for a closed month, a published
monthly figure equals `raw.finance_ledger` to the cent, per legal entity.

It runs every morning rather than at close, because a break found on the 3rd is
a day's work and a break found on the 5th is a late close. The open month has no
ledger row and no tie; those entities come back with a null and are not breaks.

What it ties over comes from `config/recon.yml`. It used to run over four
subjects and was rendered once per subject; one subject survived and the config
did not get simplified with it.

Owned by finance-analytics.
"""

from __future__ import annotations

from pathlib import Path

import pendulum
import yaml
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

from include.lib import workspace_root
from include.lib.notify import notify
from projects.finance.lib import ledger, warehouse_checks

CONFIG = workspace_root() / "config"

# A committed file, read once at parse. No network, no warehouse, no clock.
cfg = yaml.safe_load(Path(CONFIG / "recon.yml").read_text(encoding="utf-8"))

SUBJECT = ledger.subject_table(cfg)
LEDGER = cfg["ledger"]
BREAKS = cfg["breaks"]
TOLERANCE_CENTS = int(cfg.get("tolerance_cents", 0))
SKIP_BEFORE_FIRST_CLOSE = bool(cfg.get("skip_before_first_close", True))

#: The day this run owns, and the fiscal month it rolls into.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"
FISCAL_MONTH = "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y-%m') }}"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify("finance", "the ledger tie did not run or did not hold"),
}

with DAG(
    dag_id="fin_ledger_tie",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "ledger", "reconciliation"],
    doc_md=__doc__,
) as dag:
    wait_for_subject = PythonSensor(
        task_id="wait_for_subject",
        python_callable=warehouse_checks.has_partition,
        op_kwargs={"table": SUBJECT, "partition_col": "order_date",
                   "partition_value": TARGET_DS},
        poke_interval=300,
        timeout=60 * 60 * 2,
        mode="reschedule",
        doc_md="The subject is built by another team, four hours before this "
               "runs. Two hours of waiting, then say so.",
    )

    assert_total = PythonOperator(
        task_id="assert_total",
        python_callable=warehouse_checks.assert_total_matches,
        op_kwargs={
            "table": SUBJECT, "column": "booked_cents",
            "source_table": "marts.daily_revenue", "source_column": "booked_cents",
            "partition_col": "order_date", "source_partition_col": "ds",
            "partition_value": TARGET_DS,
        },
        doc_md="The day's total on the subject equals the day's booked revenue, "
               "to the cent. Two teams' numbers meeting, checked before either "
               "reaches the ledger.",
    )

    tie = PythonOperator(
        task_id="tie",
        python_callable=ledger.tie_to_ledger,
        op_kwargs={
            "table": "marts.revenue_recognized_monthly", "column": "recognized_cents",
            "ledger": LEDGER, "fiscal_month": FISCAL_MONTH,
            "tolerance_cents": TOLERANCE_CENTS,
            "skip_before_first_close": SKIP_BEFORE_FIRST_CLOSE,
        },
        doc_md="Published against booked, per entity, for the month the day "
               "falls in. An entity with no ledger row for that month has no "
               "coverage yet and is not a break.",
    )

    write_breaks = PythonOperator(
        task_id="write_breaks",
        python_callable=ledger.write_breaks,
        op_kwargs={"rows": tie.output, "table": BREAKS, "fiscal_month": FISCAL_MONTH},
        pool="warehouse_write",
        doc_md="Every entity is written, not only the breaks. A month where "
               "nothing broke is a fact worth being able to show.",
    )

    fail_on_breaks = PythonOperator(
        task_id="fail_on_breaks",
        python_callable=ledger.fail_on_breaks,
        op_kwargs={"rows": tie.output},
        doc_md="Fail after the breaks are written, never before. The record is "
               "what the close team reads at 08:30 and it has to exist whichever "
               "way this goes.",
    )

    wait_for_subject >> assert_total >> tie >> write_breaks >> fail_on_breaks
