"""Spend against attributed revenue, by channel and campaign.

Scheduled on all three of its inputs: the attribution this team builds, the
spend this team lands, and commerce's GMV. Waiting on the three assets rather
than on a clock is the difference between a return figure and a return figure
divided by a day that had not finished landing.

**Spend is a delivery, not a day.** `raw.ads_spend_daily` holds every
delivery of a report date, first and restated, so the report date has to be
resolved to one figure before anything divides by it.
`docs/late-data-policy.md` has nothing to say about it — an ad platform
restating a closed day is not late data, it is a different number for the
same day — and `int_price_compared`'s neighbours in `models/growth/` are
where the resolution lives.

**Currency.** Platforms bill in the market's own currency and the row carries
no rate, so the model joins `raw.fx_rates` at the report date. Money stays
integer cents, per the platform rule.

Owned by growth. The channel dashboards and the weekly marketing review read
`marts.channel_roi_daily`.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.growth.lib import ads, dbt
from projects.growth.lib.assets import (ADS_SPEND, CHANNEL_ATTRIBUTION,
                                        CHANNEL_ROI, GMV_DAILY)

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("growth", "channel ROI build failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="gro_channel_roi_daily",
    schedule=[CHANNEL_ATTRIBUTION, ADS_SPEND, GMV_DAILY],
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["growth", "marketing", "mart"],
    doc_md=__doc__,
)
def gro_channel_roi_daily():

    @lake_task
    def report_dates() -> list[str]:
        """The report dates whose spend has moved since we last built them."""
        return ads.report_dates_pending()

    stage_spend = dbt.selection("stage_spend", "stg_ecommerce__ads_spend")
    build_roi = dbt.selection("build_roi", "channel_roi_daily")
    price_compared = dbt.selection("price_compared", "int_price_compared")

    @lake_task
    def spend_ties() -> int:
        """Spend in the mart against spend in the feed, and the dates checked.

        The mart holds one figure per report date and the feed holds every
        delivery of it, so the two tie only if the resolution took one
        delivery per key. A gap means it took more than one, and the return
        figure is wrong by however much it double-counted.
        """
        checked = ads.spend_ties()
        if checked["gaps"]:
            raise Reject(f"{len(checked['gaps'])} report date(s) do not tie "
                         "to the feed", rows=checked["gaps"])
        return checked["report_dates"]

    @task(outlets=[CHANNEL_ROI])
    def publish_roi() -> str:
        """Announce the mart to the dashboards and the weekly review."""
        return "channel_roi_daily"

    dates = report_dates()
    ties = spend_ties()
    dates >> stage_spend >> build_roi >> [price_compared, ties] >> publish_roi()


gro_channel_roi_daily()
