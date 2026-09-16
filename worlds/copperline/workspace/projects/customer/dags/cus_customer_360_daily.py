"""`marts.customer_360`: one row per trade account, everything about it.

Contact, orders, payments, disputes, support and tenure, rebuilt nightly.
Support read it all day, from the warehouse view and from the service desk.

**This is the slowest model in the estate.** Eight joins over two years of
events and no incremental build. It takes about seven minutes a day on the
current volumes and nobody has made it incremental, which is an open item in
`contracts/customer-360.md` and not a secret.

**One row per real account.** The merge decision in `ops.merge_candidates`
applies and the account appears once; two records that look alike and were
not decided both appear. §CU-1.

**Same-day ties have a written rule.** Where two versions of an attribute
carry the same effective date, take the last by `updated_at`, and where those
tie as well, take the source with the higher priority: Copperline OMS, then
Halyard, then the acquired book. §CU-2. The rule is written down so that two
rebuilds of one day produce the same row.

Runs at 06:00, after the dimension at 04:00 and the crosswalk at 03:30, and
it has to be finished by 07:00 for support.

Owned by customer.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from include.lib import workspace_root
from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.customer.lib import profile
from projects.customer.lib.assets import CUSTOMER_360

DBT_DIR = str(workspace_root() / "dbt" / "copperline_analytics")

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify("customer", "the 360 build failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 120}}


@dag(
    dag_id="cus_customer_360_daily",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "360", "mart"],
    doc_md=__doc__,
)
def cus_customer_360_daily():

    @lake_task
    def resolved_accounts(day: str) -> int:
        """The account list this build works from, merges applied."""
        return profile.resolve_accounts(day)

    lifecycle = BashOperator(
        task_id="lifecycle",
        bash_command="dbt run --select int_customer_lifecycle "
                     "--vars '{\"run_date\": \"{{ ds }}\"}'",
        cwd=DBT_DIR,
    )

    @lake_task
    def order_history(day: str) -> int:
        """Orders, net sales and first and last order date per account.

        Trailing twelve months for `net_sales_cents`, as
        `docs/semantic-definitions.md` defines it. `orders_12m` counts orders
        and not order lines, which merchandising ask about every few months.
        """
        return profile.order_history(day)

    @lake_task
    def payment_history(day: str) -> int:
        """Terms, open balance and days-to-pay per account."""
        return profile.payment_history(day)

    @lake_task
    def dispute_history(day: str) -> int:
        """Open disputes per account, from the commerce spine."""
        return profile.dispute_history(day)

    @lake_task
    def support_history(day: str) -> int:
        """Tickets, and the last contact, over both books' parties."""
        return profile.support_history(day)

    build_360 = BashOperator(
        task_id="build_360",
        bash_command="dbt run --select customer_360 "
                     "--vars '{\"run_date\": \"{{ ds }}\"}'",
        cwd=DBT_DIR,
        retries=1,
    )

    @lake_task
    def check_contract() -> dict[str, int]:
        """Grain, columns and the hashed contact, against the contract file.

        Contact detail is hashed in the published view and the unhashed value
        stays in the warehouse, which is the line `contracts/privacy.md`
        draws and this check keeps.
        """
        breaks = profile.contract_breaks()
        if breaks:
            raise Reject(f"{len(breaks)} contract violation(s)",
                         rows=[{"violation": text} for text in breaks])
        return profile.profile_counts()

    @task(outlets=[CUSTOMER_360])
    def publish_360(day: str) -> str:
        """Announce the table, which is what `cus_360_publish` waits on."""
        return day

    day = "{{ ds }}"
    accounts = resolved_accounts(day)
    accounts >> lifecycle >> [order_history(day), payment_history(day),
                              dispute_history(day), support_history(day)] \
        >> build_360 >> check_contract() >> publish_360(day)


cus_customer_360_daily()
