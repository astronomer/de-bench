"""Weekly churn scores for the trade book.

A score per account per week: how likely the account is to stop ordering,
from recency, frequency, order value, support load and dispute history. It
runs on Friday morning so that account managers have it for the following
week's calls.

**The denominator is stated, not assumed.** An account with no orders in the
window is not a churn risk, it is an account we have never had, and
`int_customer_lifecycle` is where that distinction is made once for both
teams. This DAG reads the lifecycle model; it does not invent a second rule
about who counts.

**Both books, one score.** Acquired accounts are scored on their own history
and carry `source_book`, so a manager can see whether the number is built on
two years of Copperline orders or on eight months since the book landed.
Anything reading dates before the acquisition states the era, and the model
description does.

**The score is a number, not a decision.** There is no threshold in this DAG
and no list of accounts to call. Whoever reads the table applies one.

Owned by customer.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from include.lib import calendar as retail_calendar
from include.lib import workspace_root
from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.customer.lib import churn

DBT_DIR = str(workspace_root() / "dbt" / "copperline_analytics")

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "churn scoring failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_churn_scores_weekly",
    schedule="0 9 * * 5",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "churn", "mart"],
    doc_md=__doc__,
)
def cus_churn_scores_weekly():

    @lake_task
    def scoring_week(day: str) -> dict[str, str]:
        """The fiscal week being scored, from the retail calendar.

        Copperline reports on a 4-5-4 calendar, so the week a Friday belongs
        to is the one the calendar says and not the one date arithmetic
        implies.
        """
        return {"week_start": retail_calendar.week_start(day).isoformat(),
                "fiscal_week": str(retail_calendar.fiscal_week(day)),
                "fiscal_year": retail_calendar.fiscal_year(day)}

    lifecycle = BashOperator(
        task_id="lifecycle",
        bash_command="dbt run --select int_customer_lifecycle "
                     "--vars '{\"run_date\": \"{{ ds }}\"}'",
        cwd=DBT_DIR,
    )

    @lake_task
    def features(day: str) -> int:
        """Recency, frequency, value, support load and disputes per account."""
        return churn.build_features(day)

    @lake_task
    def score(day: str) -> int:
        """Score each account and write `marts.churn_scores_weekly`."""
        return churn.build_scores(day)

    @lake_task
    def check_scores(day: str) -> dict[str, int]:
        """One score per account per week, every score between zero and one.

        A score outside the range is arithmetic going wrong, and it is worth
        catching here: a manager reading a list sorted by score would find
        the broken rows at the top.
        """
        problems = churn.score_problems(day)
        if problems:
            raise Reject(f"{len(problems)} score(s) look wrong", rows=problems)
        return churn.score_counts(day)

    @task
    def publish_scores(day: str) -> str:
        """Write the week's partition for the account managers' report."""
        return churn.publish(day)

    day = "{{ ds }}"
    scoring_week(day) >> lifecycle >> features(day) >> score(day) \
        >> check_scores(day) >> publish_scores(day)


cus_churn_scores_weekly()
