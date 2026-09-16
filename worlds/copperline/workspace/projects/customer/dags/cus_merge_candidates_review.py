"""`ops.merge_candidates`, from the integration team's review sheets.

Three hundred pairs of accounts, each one looked at by a person during the
integration and marked as the same customer or not. The reviewers worked in
spreadsheets that still land under `landing/crm/merge_review/`, and this DAG
folds them into the table everything downstream reads.

**Only decided pairs reach the table.** A pair somebody looked at and
rejected is not written, which is what makes the table's presence mean
something: an account in it was decided to be a match. An account absent from
it might have been reviewed and rejected or might never have been looked at,
and the table cannot tell you which. That is an open item in the runbook, not
a bug here.

**Nothing re-derives a match.** No name comparison, no address distance, no
shared-domain rule. The review is the decision, and matching the population
again produces a different set: the same customers trade under two names,
different businesses share a domain, and `info@` addresses and resellers are
everywhere. `docs/runbooks/northwave-integration.md` §NWI-1.

Owned by customer. Nobody owns the merge any more; the integration team were
disbanded into the market-setup work.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.customer.lib import merges

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "merge review load failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_merge_candidates_review",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "northwave", "identity"],
    doc_md=__doc__,
)
def cus_merge_candidates_review():

    @task
    def review_sheets() -> list[str]:
        """Every review sheet under `landing/crm/merge_review/`."""
        return merges.review_sheets()

    @lake_task
    def load_reviews(sheets: list[str]) -> int:
        """Read the sheets into `ops.merge_review_inbox`, whole."""
        return merges.load_reviews(sheets)

    @lake_task
    def apply_decisions() -> int:
        """Write the decided pairs, and only those, to
        `ops.merge_candidates`."""
        return merges.apply_decisions()

    @lake_task
    def check_pairs() -> dict[str, int]:
        """One decision per pair, and no account decided twice.

        An account on either book that appears in two pairs would be merged
        into two different customers, and the dimension would then hold the
        same history twice under two ids.
        """
        problems = merges.pair_problems()
        if problems:
            raise Reject(f"{len(problems)} account(s) decided more than once",
                         rows=problems)
        return merges.decision_counts()

    @task
    def review_coverage() -> dict[str, int]:
        """How much of the acquired book has been looked at.

        Reported rather than enforced. The review happened once and is not
        going to happen again, so this number is a fact about the estate
        rather than a target.
        """
        return merges.coverage()

    sheets = review_sheets()
    load_reviews(sheets) >> apply_decisions() >> [check_pairs(),
                                                  review_coverage()]


cus_merge_candidates_review()
