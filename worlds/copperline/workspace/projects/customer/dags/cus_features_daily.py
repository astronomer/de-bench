"""The Compass feature table: `marts.feature_customer_daily`, and its parquet.

Consumer C-9, bound by `contracts/feature-store.md`. One row per customer per
day, written to `exports/features/{ds}.parquet` for the modelling team to
pick up. Due at 09:00; this runs at 08:00.

**Every value on a row is the value that was true at the row's event time.**
§FS-1. Account attributes come from a slowly-changing dimension, and the join
takes the version in force at the event time, through the dimension's
validity window. Joining the current version instead runs, produces a full
table, and scores better — and a feature built from a value the account did
not yet have will not exist when the model is asked to predict.

**Trailing windows end on the day, inclusive.** Seven, thirty and ninety
days, all inclusive of `ds`, because that is how Compass was trained.
Changing it means retraining, so it is written here rather than assumed.

**A day is written once.** Re-running a day replaces its file. The parquet
files are a surface a deletion request must reach, per
`docs/retention-policy.md` §RET-4, and they are on the list in
`contracts/privacy.md`.

Owned by customer.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from include.lib import workspace_root
from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.customer.lib import features

DBT_DIR = str(workspace_root() / "dbt" / "copperline_analytics")

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "the feature table failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_features_daily",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "features", "export"],
    doc_md=__doc__,
)
def cus_features_daily():

    @lake_task
    def attribute_versions(day: str) -> int:
        """The dimension version in force at each account's event time.

        Written out as its own step so that the as-of join is visible in the
        graph rather than buried in a model. Filtering the dimension to its
        current version instead is the short way to a full table and is what
        §FS-1 forbids.
        """
        return features.attributes_as_of(day)

    @lake_task
    def behaviour_windows(day: str) -> int:
        """Orders and net sales over the seven, thirty and ninety day
        windows, each one ending on the day inclusive."""
        return features.behaviour_windows(day)

    build_features = BashOperator(
        task_id="build_features",
        bash_command="dbt run --select feature_customer_daily "
                     "--vars '{\"run_date\": \"{{ ds }}\"}'",
        cwd=DBT_DIR,
    )

    @lake_task
    def check_as_of(day: str) -> dict[str, int]:
        """No feature row carries an attribute the account did not yet have.

        The check compares each row's attribute against the dimension version
        valid at the row's event time. A row that matches only the current
        version is a leak from the future, and it is exactly what a model
        trained on it cannot reproduce.
        """
        leaks = features.as_of_leaks(day)
        if leaks:
            raise Reject(f"{len(leaks)} feature row(s) carry a later "
                         "attribute version", rows=leaks)
        return features.feature_counts(day)

    @lake_task
    def write_parquet(day: str) -> str:
        """Write `exports/features/<ds>.parquet`, replacing the day's file."""
        return features.write_export(day)

    @task
    def check_contract(day: str) -> list[str]:
        """The mart against `contracts/feature_customer_daily.yml`."""
        return features.contract_breaks()

    day = "{{ ds }}"
    [attribute_versions(day), behaviour_windows(day)] >> build_features
    build_features >> check_as_of(day) >> write_parquet(day) >> check_contract(day)


cus_features_daily()
