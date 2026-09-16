"""`marts.dim_customer`: one row per real trade account, over both books.

The conformed customer dimension. It is built here rather than by the
platform team, which is not what the layering says and is the way it has
been since the acquisition. The contract it has to satisfy is the platform's,
in `contracts/customer_360.yml`'s neighbours, and this DAG is what keeps it
satisfied.

**One row per real account, not per account record.** Where the review in
`ops.merge_candidates` says two records are the same customer, the Copperline
record survives and carries the acquired id in `merged_from`. Where two
records look alike and nobody decided, both stay. The merge comes from that
table and is never re-derived: matching names, addresses or domains produces
a different set, and the population was chosen because it is hard.

**`source_book` stays on the row after a merge**, carrying the book the
surviving record came from. Everything downstream keys on it.

**A deleted account is not dropped.** It leaves the published view as
`closed` and stays reconstructable, per `docs/retention-policy.md` §RET-6.

Runs at 04:00, after the crosswalk at 03:30 and before the 360 at 06:00.

Owned by customer. The board pack, the 360 and the feature table all read
this dimension.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.customer.lib import identity
from projects.customer.lib.assets import DIM_CUSTOMER

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "customer dimension build failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_dim_customer_daily",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "identity", "dimension"],
    doc_md=__doc__,
)
def cus_dim_customer_daily():

    @lake_task
    def decided_pairs() -> int:
        """The reviewed merges this build will apply.

        Read first, so that the number the run used is on the record. A pair
        added to the review yesterday changes the dimension today, and this
        is where that shows.
        """
        return len(identity.merge_pairs())

    @lake_task
    def build_dim(day: str) -> int:
        """Build the day's dimension over both books."""
        return identity.build_dim(day)

    @lake_task
    def check_grain(day: str) -> dict[str, int]:
        """One row per account, both books accounted for.

        A merged acquired account that also stands on its own would put the
        same customer's history under two ids, and every per-customer figure
        would then be split between them.
        """
        problems = identity.dim_problems(day)
        if problems:
            raise Reject(f"{len(problems)} problem(s) in the dimension",
                         rows=problems)
        return identity.dim_counts(day)

    @lake_task
    def check_contract(day: str) -> list[str]:
        """The dimension against the contract file the platform team keeps.

        The contract is the machine half of the agreement, and it is checked
        here as well as by `plat_contracts_enforce` in the morning, so that a
        break is found by the team that caused it.
        """
        return identity.contract_breaks(day)

    @lake_task
    def unmatched_book(day: str) -> dict[str, int]:
        """How much of the acquired book stands on its own in the dimension.

        Eleven hundred accounts, give or take. They are not errors — the
        books were never merged and only three hundred pairs were ever
        decided — but a large move in the number means a decision changed.
        """
        return identity.book_split(day)

    @task(outlets=[DIM_CUSTOMER])
    def publish_dim(day: str) -> str:
        """Announce the dimension to the 360 and the feature build."""
        return day

    day = "{{ ds }}"
    built = build_dim(day)
    decided_pairs() >> built
    built >> [check_grain(day), check_contract(day), unmatched_book(day)] \
        >> publish_dim(day)


cus_dim_customer_daily()
