"""The morning ad-spend deliveries, landed into `raw.ads_spend_daily`.

Three platforms drop a CSV each at about 04:00 and we land them at 05:00.
Larkspur drops its e-mail export in the same folder and goes to a different
table, because it reports events and never a spend row.

What is inside a delivery is another matter: a file dropped this morning
holds yesterday's first report and, three to seven days behind it,
restatements of report dates we already have. Both stay.
`projects/growth/lib/ads.py` says why and holds the write.

One platform per task, so a platform that drops a bad file fails alone and
retries alone. The house CSV loader is not used here: its replace scopes to
one column, and this load has to be scoped to the platform and the delivery
date together.

The delivery date is recorded once, by `delivery_date`, and every step below
reads it back out of the manifest for its own interval. Taking it as a task
argument instead hands the step the newest morning the DAG ever ran, which
is this run's own morning going forward and somebody else's on a replay.

Owned by growth. `marts.channel_roi_daily` and the channel dashboards read
what this lands.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, get_current_context, task

from include.lib.notify import notify
from include.lib.pipeline import lake_task, manifest
from projects.growth.lib import ads
from projects.growth.lib.assets import ADS_SPEND

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify("growth", "ad spend delivery missing",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 120}}


@dag(
    dag_id="gro_marketing_spend_intake",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["growth", "marketing", "intake"],
    doc_md=__doc__,
)
def gro_marketing_spend_intake():

    @lake_task
    def delivery_date() -> str:
        """The morning this run owns. Read once, here, and recorded."""
        return str(get_current_context()["ds"])

    @lake_task(map_index_template="{{ task.op_kwargs['platform'] }}")
    def land_spend(platform: str) -> int:
        """Land one platform's delivery for the morning."""
        return ads.land_delivery(platform, _day())

    @lake_task
    def land_email() -> int:
        """Land Larkspur's bulk e-mail export for the morning."""
        return ads.land_email(_day())

    @lake_task
    def restatements() -> list[dict]:
        """What the morning's deliveries moved, key by key.

        A restatement corrects a report date we already hold, so the pair is
        reported here — the first delivery's spend beside the new one —
        rather than left for somebody to find in a moved total.
        """
        return ads.restated_keys(_day())

    @lake_task
    def fx_coverage() -> list[dict]:
        """Currencies the morning's deliveries use that have no rate to
        convert on.

        Nothing here converts. Every shared-currency figure is a join to
        `raw.fx_rates` at the report date, so a gap is counted on the way in
        rather than found later in a total that does not tie.
        """
        return ads.missing_rates(_day())

    @task(outlets=[ADS_SPEND])
    def publish_spend(day: str) -> str:
        """Announce the morning's deliveries to the marts downstream."""
        return day

    recorded = delivery_date()
    landed = land_spend.expand(platform=list(ads.SPEND_PLATFORMS))
    recorded >> landed
    landed >> [restatements(), fx_coverage()] >> publish_spend("{{ ds }}")
    landed >> land_email()


def _day() -> str:
    """The morning `delivery_date` recorded for THIS run's interval."""
    return str(manifest.get("delivery_date"))


gro_marketing_spend_intake()
