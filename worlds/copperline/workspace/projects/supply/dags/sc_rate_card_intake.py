"""The carrier rate cards, landed weekly.

Each carrier drops a card file into `landing/carriers/rate_cards/`, named for
the day it arrived rather than for the day its card takes effect. One row per
carrier, service level and weight break, plus the weekly fuel series beside it.

**This DAG lands what the carrier sent.** It does not check the version against
`docs/rate-policy.md` RATE-1, which is the table that actually decides which
card is in force. That table has been right and the file wrong twice: once when
a card was backdated by a month, once when a card arrived with no end date. A
backdated card lands here silently and stays until somebody notices a cost step.

Produces `raw.carrier_rate_cards` and `raw.carrier_fuel_surcharges`. Read by
`sc_shipping_cost_daily` and by finance's freight accrual.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.loaders import CsvToWarehouseOperator
from include.lib.notify import notify
from projects.supply.lib import paths

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=15),
}

CARD_COLUMNS = {
    "card_version": "VARCHAR",
    "carrier_code": "VARCHAR",
    "service_level": "VARCHAR",
    "break_seq": "INTEGER",
    "upper_bound_g": "BIGINT",
    "break_kg": "INTEGER",
    "base_cents": "BIGINT",
    "zone_step_cents": "BIGINT",
    "weight_step_cents": "BIGINT",
    "dim_divisor": "INTEGER",
    "zone_matrix_json": "VARCHAR",
    "effective_from": "DATE",
    "effective_to": "DATE",
    "landed_on": "DATE",
}

BREAK_ORDER = """
-- The breaks on a card ascend, and the last one has no upper bound above it.
-- A card whose breaks are out of order rates every heavy package in the wrong
-- band and nothing about the arithmetic complains.
SELECT card_version, carrier_code, service_level
FROM copperline.raw.carrier_rate_cards
WHERE landed_on = ?::DATE
GROUP BY card_version, carrier_code, service_level
HAVING max(break_seq) <> count(*)
    OR min(upper_bound_g) <= 0
ORDER BY carrier_code, service_level
"""


def check_break_order(ds: str) -> int:
    """Every card's weight breaks ascend from one, with no gaps."""
    with warehouse.connect(read_only=True) as con:
        broken = con.execute(BREAK_ORDER, [ds]).fetchall()
    if broken:
        names = ", ".join(f"{row[1]} {row[2]} ({row[0]})" for row in broken[:5])
        raise ValueError(f"{ds}: these cards have broken weight breaks: {names}")
    return 0


with DAG(
    dag_id="sc_rate_card_intake",
    schedule="0 6 * * 1",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "raw", "intake", "freight"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "rate card intake"),
) as dag:
    land_cards = CsvToWarehouseOperator(
        task_id="land_rate_cards",
        source=paths.RATE_CARD_TEMPLATE,
        table="raw.carrier_rate_cards",
        mode="replace",
        partition_col="landed_on",
        columns=CARD_COLUMNS,
    )

    land_fuel = CsvToWarehouseOperator(
        task_id="land_fuel_surcharges",
        source=paths.FUEL_TEMPLATE,
        table="raw.carrier_fuel_surcharges",
        mode="replace",
        partition_col="landed_on",
        columns={"carrier_code": "VARCHAR", "week_start": "DATE",
                 "fuel_pct_bps": "INTEGER", "landed_on": "DATE"},
    )

    breaks = PythonOperator(
        task_id="check_break_order", python_callable=check_break_order,
        op_kwargs={"ds": "{{ ds }}"})

    land_cards >> land_fuel >> breaks
