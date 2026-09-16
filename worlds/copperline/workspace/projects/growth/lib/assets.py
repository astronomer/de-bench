"""Every asset growth publishes or waits on, named once.

`CONVENTIONS.md` says a URI is written here and nowhere else. The reason is
dull and expensive: an asset is matched by its URI string, so a DAG that
spells one differently subscribes to nothing, waits forever and reports
nothing wrong. Naming each one here makes the spelling a single fact.

Two groups. The first is what this team publishes — a task in a growth DAG
carries one of these in `outlets`, and every subscriber upstream of it is in
this file too. The second is what other teams publish and we read; those come
to us through `plat_asset_republish`, which is what puts a cross-team mart on
the asset graph at all.

Nothing here is a rendered DAG's output. A blueprint step takes no `outlets`,
so `projects/growth/dags/*.dag.yaml` publishes no asset and anything waiting
on one of those waits on a clock instead.
"""

from __future__ import annotations

from airflow.sdk import Asset

__all__ = [
    "WEB_EVENTS",
    "WEB_SESSIONS",
    "CHANNEL_ATTRIBUTION",
    "CHANNEL_ROI",
    "MARKET_PRICING",
    "FUNNEL_DAILY",
    "ADS_SPEND",
    "AUDIENCE_SEGMENTS",
    "GMV_DAILY",
    "SELL_THROUGH",
]

# --- what growth publishes -------------------------------------------------

#: Driftwood's raw event stream, landed hourly by `gro_clickstream_intake`.
WEB_EVENTS = Asset("duckdb://warehouse/raw.web_events")

#: Sessions stitched from the event stream by `gro_sessionize_daily`.
WEB_SESSIONS = Asset("duckdb://warehouse/marts.fct_web_sessions")

#: Touches attributed to orders by `gro_attribution_daily`.
CHANNEL_ATTRIBUTION = Asset("duckdb://warehouse/marts.agg_channel_funnel_daily")

#: Spend against attributed revenue, by `gro_channel_roi_daily`.
CHANNEL_ROI = Asset("duckdb://warehouse/marts.channel_roi_daily")

#: Price and margin by market, by `gro_pricing_mart_daily`.
MARKET_PRICING = Asset("duckdb://warehouse/marts.market_pricing")

#: Funnel steps by day, by `gro_funnel_daily`.
FUNNEL_DAILY = Asset("duckdb://warehouse/marts.funnel_daily")

#: Every ad-platform delivery, landed by `gro_marketing_spend_intake`.
ADS_SPEND = Asset("duckdb://warehouse/raw.ads_spend_daily")

# --- what we wait on ------------------------------------------------------

#: The segment membership the nightly dbt build produces. The two reverse-ETL
#: DAGs and `gro_audience_export` all hang off it.
AUDIENCE_SEGMENTS = Asset("duckdb://warehouse/marts.audience_segments")

#: Commerce's marketplace and web order value. `channel_roi_daily` divides by
#: it, so it is half of that DAG's schedule.
GMV_DAILY = Asset("duckdb://warehouse/marts.gmv_daily")

#: Supply's sell-through. The pricing mart reads it for realised price.
SELL_THROUGH = Asset("duckdb://warehouse/marts.sell_through_daily")
