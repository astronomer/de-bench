"""Push the day's audiences to Beacon Ads and Tessera Social.

This is consumer C-6 and `contracts/audience-sync.md` binds it. Same shape as
its sibling `gro_reverse_etl_crm`: same schedule, same consent filter, same
idempotence by segment key. Change one and change the other.

**An empty segment fails the run rather than rejecting the batch.** §AS-1 is
explicit: an empty push clears the audience at the ad platform and stops the
campaign, and nothing downstream can tell "this audience is empty" from "the
audience was not built". A rejected batch would skip the publish and leave
the run green, so this one raises instead.

**The publish step is a shell script, not an operator.**
`scripts/publish_reports.sh` is a leftover from before these DAGs existed and
it has never been rewritten. It reports what it pushed, and it reports
success on zero segments — which is why `confirm_delivery` reads the counts
back afterwards rather than trusting the exit status.

Owned by growth. A missed audience shows up first as a campaign that stops
spending, on somebody else's dashboard.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from include.lib import workspace_root
from include.lib.notify import notify
from include.lib.pipeline import lake_task
from projects.growth.lib import segments
from projects.growth.lib.assets import AUDIENCE_SEGMENTS

#: The two ad destinations, as `projects/growth/config/segments.yml` names
#: them.
DESTINATIONS = ("beacon_ads", "tessera_social")

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify("growth", "audience sync failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 3, "delay_seconds": 120}}


@dag(
    dag_id="gro_reverse_etl_ads",
    schedule=[AUDIENCE_SEGMENTS],
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["growth", "audiences", "reverse-etl"],
    doc_md=__doc__,
)
def gro_reverse_etl_ads():

    @task
    def publish_day() -> str:
        """The day being pushed.

        An asset-triggered run has no interval to read, so the day comes from
        the mart: the newest `ds` it holds is the day that has just been
        built. A plain task, so the publish step can template it.
        """
        return segments.latest_segment_day()

    @lake_task
    def collect(day: str) -> dict[str, int]:
        """How many rows each destination's segments have, consent applied."""
        wanted = segments.destinations()
        return {name: len(segments.segment_rows(day, wanted[name]["segments"]))
                for name in DESTINATIONS}

    @lake_task(map_index_template="{{ task.op_kwargs['destination'] }}")
    def stage_files(destination: str, day: str) -> list[str]:
        """Write one destination's files, or fail on an empty segment."""
        spec = segments.destinations()[destination]
        rows = segments.segment_rows(day, spec["segments"])
        empty = segments.empty_segments(rows, spec["segments"])
        if empty:
            raise ValueError(
                f"{destination}: no rows for {', '.join(empty)}; delivering "
                "an empty audience would clear it at the platform"
            )
        return segments.stage(destination, spec, day, rows)

    publish = BashOperator(
        task_id="publish",
        bash_command='scripts/publish_reports.sh audiences "$PUBLISH_DAY"',
        env={"PUBLISH_DAY": "{{ ti.xcom_pull(task_ids='publish_day') }}"},
        append_env=True,
        cwd=str(workspace_root()),
    )

    @lake_task
    def confirm_delivery(day: str) -> dict[str, int]:
        """What the platforms hold, against what we sent.

        The script's exit status is not the delivery. Zero segments delivered
        is a failed run whatever it said, which is the second half of §AS-1.
        """
        delivered = segments.delivered_counts(DESTINATIONS, day)
        missing = [name for name in DESTINATIONS if not delivered.get(name)]
        if missing:
            raise ValueError(f"nothing reached {', '.join(missing)}")
        return delivered

    day = publish_day()
    collect(day) >> stage_files.partial(day=day).expand(
        destination=list(DESTINATIONS)
    ) >> publish >> confirm_delivery(day)


gro_reverse_etl_ads()
