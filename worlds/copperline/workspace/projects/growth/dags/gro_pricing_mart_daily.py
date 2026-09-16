"""`marts.market_pricing`: price and margin by market.

Scheduled on supply's sell-through, because realised price is what sold and
that is where it is counted. The competitor index lands at 07:00 on its own
clock and is read as it stands; a market with no index for the day carries a
null index rather than dropping out of the mart, so the market list does not
change shape when a competitor feed is late.

**Markets come from the seed, and the seed is a shorter list than the world.**
`seeds/markets.csv` is what the pricing models build from and
`raw.market_config` is the record of which markets exist.
`docs/runbooks/market-setup.md` §MKT-1 owns that distinction and
`gro_market_seed_build` is what moves a market from the second list to the
first.

Money is integer cents throughout, and a market's own currency is converted
through `raw.fx_rates` at the trading date rather than at today's rate.

Owned by growth. Merchandising read this mart every morning.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.growth.lib import dbt, pricing
from projects.growth.lib.assets import MARKET_PRICING, SELL_THROUGH

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("growth", "pricing mart build failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="gro_pricing_mart_daily",
    schedule=[SELL_THROUGH],
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["growth", "pricing", "mart"],
    doc_md=__doc__,
)
def gro_pricing_mart_daily():

    @lake_task
    def markets_built() -> list[str]:
        """The markets the seed puts in the marts, in order."""
        return pricing.seeded_markets()

    stage_prices = dbt.selection("stage_prices", "stg_product__prices")
    price_compared = dbt.selection("price_compared", "int_price_compared")
    build_pricing = dbt.selection("build_pricing", "market_pricing")

    @lake_task
    def margin_sanity(markets: list[str]) -> dict[str, int]:
        """Every seeded market has a row, and no margin is impossible.

        A market that drops out of the mart looks like a market with no
        sales, which is a story somebody will believe. A margin outside minus
        one hundred to one hundred per cent is arithmetic, not trading.
        """
        problems = pricing.pricing_problems(markets)
        if problems:
            raise Reject(f"{len(problems)} market row(s) look wrong",
                         rows=problems)
        return {"markets": len(markets)}

    @task(outlets=[MARKET_PRICING])
    def publish_pricing() -> str:
        """Announce the mart to merchandising's dashboards."""
        return "market_pricing"

    markets = markets_built()
    markets >> stage_prices >> price_compared >> build_pricing
    build_pricing >> margin_sanity(markets) >> publish_pricing()


gro_pricing_mart_daily()
