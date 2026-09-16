"""Marketing consent state, from Halyard into `marts.consent_daily`.

One row per account per day: whether marketing consent is current, when it
was last granted or withdrawn, and which channel it covers. Growth's audience
sync reads the state off this, and a withdrawn account leaves the next
delivery.

**Consent is a state, not an event, and it is read as of the day.** An
account that withdrew consent in March is withdrawn on every day after March
and granted on every day before it. Reading the newest event and applying it
to history would rewrite what we were allowed to do last year, which is the
one thing a consent record must never do.

**A deletion request outranks consent.** An account with a request in flight
is out of every delivery whatever its consent says, per
`docs/retention-policy.md` §RET-4. The flag is carried here so that growth's
side has one column to filter on rather than two lists to join.

Owned by customer.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.customer.lib import consent

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "consent sync failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_consent_sync_daily",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "consent", "mart"],
    doc_md=__doc__,
)
def cus_consent_sync_daily():

    @lake_task
    def land_events(day: str) -> int:
        """Land the day's consent events, whole."""
        return consent.land_events(day)

    @lake_task
    def build_state(day: str) -> int:
        """The state of each account's consent as of the day.

        The newest event on or before the day wins; an account with no event
        has never been asked and is not granted.
        """
        return consent.build_state(day)

    @lake_task
    def apply_deletions(day: str) -> int:
        """Mark accounts with a deletion request in flight.

        The flag rides on the same row so that growth's audience filter is
        one column rather than a join to a queue in another team's project.
        """
        return consent.apply_deletions(day)

    @lake_task
    def check_state(day: str) -> dict[str, int]:
        """One row per account, and no state that is neither value.

        A third value in `consent_state` would pass every filter written as
        `<> 'withdrawn'` and fail every filter written as `= 'granted'`, so
        two consumers would disagree about the same account.
        """
        problems = consent.state_problems(day)
        if problems:
            raise Reject(f"{len(problems)} consent row(s) look wrong",
                         rows=problems)
        return consent.state_counts(day)

    day = "{{ ds }}"
    land_events(day) >> build_state(day) >> apply_deletions(day) \
        >> check_state(day)


cus_consent_sync_daily()
