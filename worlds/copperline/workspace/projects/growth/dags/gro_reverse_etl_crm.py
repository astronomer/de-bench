"""Push the day's segments back into Halyard, so the CRM knows them.

Scheduled on `marts.audience_segments`, like its sibling
`gro_reverse_etl_ads`. The two are deliberately the same shape: same
schedule, same consent filter, same idempotence by segment key, same
empty-segment rule. Change one and change the other.

**Idempotent by segment key.** The push is an upsert on
`(segment_id, customer_id)`, so a run that fires twice writes the same
membership twice and Halyard holds one copy. There is no delta path: segment
membership changes every day and there is no history to diff against.

**Consent travels with the row.** `projects/growth/lib/segments.py` filters
on it once, for both destinations, so neither can forget.

Owned by growth. Sales read the segment on an account in Halyard, so a
failure here is visible to people outside the data team by mid-morning.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.growth.lib import segments
from projects.growth.lib.assets import AUDIENCE_SEGMENTS

#: The destination this DAG owns, as `config/segments.yml` names it.
DESTINATION = "halyard_crm"

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify("growth", "CRM segment push failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 3, "delay_seconds": 120}}


@dag(
    dag_id="gro_reverse_etl_crm",
    schedule=[AUDIENCE_SEGMENTS],
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["growth", "audiences", "reverse-etl"],
    doc_md=__doc__,
)
def gro_reverse_etl_crm():

    @lake_task
    def destination() -> dict:
        """Halyard's entry in the segment config: segments, and its own
        column names for the identifiers it matches on."""
        return segments.destinations()[DESTINATION]

    @lake_task
    def collect(spec: dict) -> list[dict]:
        """The day's membership for Halyard's segments, consent applied."""
        return segments.segment_rows(_publish_date(), spec["segments"])

    @lake_task
    def check_segments(spec: dict, rows: list[dict]) -> dict[str, int]:
        """A segment with no rows is a failure, not a delivery.

        An empty push clears the audience at the destination and nothing
        downstream can tell that from an audience that was never built.
        `contracts/audience-sync.md` §AS-1.
        """
        empty = segments.empty_segments(rows, spec["segments"])
        if empty:
            raise Reject(f"empty segment(s): {', '.join(empty)}",
                         rows=[{"segment_id": segment} for segment in empty])
        return segments.rows_by_segment(rows)

    @lake_task
    def push(spec: dict, rows: list[dict]) -> int:
        """Upsert the membership into Halyard, keyed by segment and account."""
        return segments.push(DESTINATION, spec, rows)

    @task
    def record_push(pushed: int) -> str:
        """What went where, for the morning's on-call summary."""
        return f"{DESTINATION}: {pushed} rows for {_publish_date()}"

    spec = destination()
    rows = collect(spec)
    pushed = push(spec, rows)
    check_segments(spec, rows) >> pushed >> record_push(pushed)


def _publish_date() -> str:
    """The day being pushed.

    An asset-triggered run has no interval to read, so the day comes from the
    mart: the newest `ds` it holds is the day that has just been built.
    """
    return segments.latest_segment_day()


gro_reverse_etl_crm()
