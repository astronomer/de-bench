"""Freight cost per package, one partition a day, into `marts.fct_shipping_costs`.

Copperline rates every package itself rather than taking the carrier's invoice
as read. `docs/rate-policy.md` is the authority and this DAG is that document in
order:

- RATE-1 picks the card version in force, and the policy's table decides when
  the card row's own `effective_from` disagrees with it.
- RATE-2 makes `effective_from` inclusive and `effective_to` exclusive, so a
  package shipped on a card's end date rates on the next card.
- RATE-3 looks the package up on its zone and its weight break. The break is the
  first whose upper bound is at or above the billable weight, so a package
  exactly on a break rates in the lower band.
- RATE-4 rounds per package, half up to the cent, then sums. Never the other way
  round.

**Reworked for the FY2026 Q1 reprice (June, K. Duffy).** Two changes, both to
make ninety days of re-rating affordable:

- The card lookup is pinned to `REPRICE_CARD_AS_OF` rather than repeated per
  package. Every day in the window rates on the card that opens the window, so
  one date does for all of them.
- The day's packages are selected on `ship_time_utc`, which is indexed, rather
  than on `ship_date`, which is not. The run's own day at the origin facility
  is midnight local and the twenty-four hours after it, and `raw.lanes` carries
  the origin's zone name, so the interval is worked out per lane.

**One day, one partition, and the day is the run's own.** The build is a whole
day of packages at a time and the write replaces that day. Nine thousand
packages a day inside the repricing window and about three thousand outside it,
so a day costs what a day costs and a range costs that times the days in it.

Produces one partition of `marts.fct_shipping_costs` and one published file per
carrier. Read by `fin_accrual_freight`, `sc_freight_accrual_workbook` and the
lane performance build.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.supply.lib import paths

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: Where a rejected batch's rows go, and what the wrapper retries.
LAKE_CONFIG = {
    "reject_table": "ops.rate_rejects",
    "retry": {"attempts": 2, "delay_seconds": 30},
}

#: The card era the FY2026 Q1 reprice was raised under. The window opens on
#: 2026-02-01, the day Brightline reissued, so every day in it sits under one
#: set of cards and the lookup does not have to be repeated per package.
REPRICE_CARD_AS_OF = "2026-02-01"

MART_DDL = """
CREATE TABLE IF NOT EXISTS copperline.marts.fct_shipping_costs (
    ds DATE NOT NULL,
    package_id VARCHAR NOT NULL,
    shipment_id VARCHAR NOT NULL,
    carrier_code VARCHAR NOT NULL,
    service_level VARCHAR NOT NULL,
    lane_id VARCHAR NOT NULL,
    origin_dc VARCHAR NOT NULL,
    zone INTEGER NOT NULL,
    billed_weight_g BIGINT NOT NULL,
    card_version VARCHAR NOT NULL,
    rated_cents BIGINT NOT NULL,
    billed_cents BIGINT NOT NULL
)
"""

VARIANCE_DDL = """
CREATE TABLE IF NOT EXISTS copperline.ops.freight_variance (
    ds DATE NOT NULL,
    carrier_code VARCHAR NOT NULL,
    rated_cents BIGINT NOT NULL,
    billed_cents BIGINT NOT NULL,
    packages BIGINT NOT NULL
)
"""

CARD_IN_FORCE = """
SELECT card_version, carrier_code
FROM copperline.raw.carrier_rate_cards
WHERE effective_from <= ?::DATE
  AND (effective_to IS NULL OR effective_to > ?::DATE)
GROUP BY card_version, carrier_code
ORDER BY carrier_code, card_version
"""

RATE_PACKAGES = f"""
-- One rated row per package. The card table holds one row per carrier, service
-- level and weight break, so the break is chosen here rather than in a second
-- lookup: the first break whose upper bound is at or above the billable weight,
-- which is why a package exactly on a break rates in the lower band.
CREATE OR REPLACE TABLE copperline.staging.rated_packages AS
WITH shipped AS (
    -- The run's own day at the origin facility, as an interval on the UTC
    -- clock: midnight local, and the twenty-four hours after it.
    SELECT p.*
    FROM copperline.raw.shipment_packages p
    JOIN copperline.raw.lanes l ON l.lane_id = p.lane_id
    WHERE p.ship_time_utc >= (?::DATE::TIMESTAMP AT TIME ZONE l.origin_tz_name)
                                 AT TIME ZONE 'UTC'
      AND p.ship_time_utc < ((?::DATE::TIMESTAMP AT TIME ZONE l.origin_tz_name)
                                 AT TIME ZONE 'UTC') + INTERVAL 24 HOUR
)
SELECT p.package_id, p.shipment_id, p.carrier_code, p.service_level,
       p.lane_id, p.origin_dc, p.ship_date, p.zone, p.billed_weight_g,
       c.card_version,
       round(c.base_cents
             + p.zone * c.zone_step_cents
             + c.break_kg * c.weight_step_cents) AS rated_cents,
       p.billed_cents,
       p.accessorial_cents
FROM shipped p
JOIN copperline.raw.carrier_rate_cards c
  ON c.carrier_code = p.carrier_code
 AND c.service_level = p.service_level
 AND c.effective_from <= DATE '{REPRICE_CARD_AS_OF}'
 AND (c.effective_to IS NULL OR c.effective_to > DATE '{REPRICE_CARD_AS_OF}')
 AND c.break_seq = (
     SELECT min(b.break_seq)
     FROM copperline.raw.carrier_rate_cards b
     WHERE b.card_version = c.card_version
       AND b.carrier_code = c.carrier_code
       AND b.service_level = c.service_level
       AND p.billed_weight_g <= b.upper_bound_g)
"""

UNRATED = """
-- Packages the card lookup did not reach. A package with no card has no cost,
-- and guessing one is worse than parking it.
SELECT p.package_id, p.carrier_code, p.service_level, p.zone,
       p.billed_weight_g, p.ship_date
FROM copperline.raw.shipment_packages p
LEFT JOIN copperline.staging.rated_packages r ON r.package_id = p.package_id
WHERE p.ship_date = ?::DATE AND r.package_id IS NULL
ORDER BY p.package_id
"""

SURCHARGES = """
-- Fuel is a percentage of the base charge, set weekly by each carrier, and it
-- is computed on the ROUNDED package charge. Accessorials are flat amounts and
-- are added after it, each rounded half up to the cent first.
UPDATE copperline.staging.rated_packages r
SET rated_cents = r.rated_cents
                + round(r.rated_cents * f.fuel_pct_bps / 10000.0)
                + r.accessorial_cents
FROM copperline.raw.carrier_fuel_surcharges f
WHERE f.carrier_code = r.carrier_code
  AND f.week_start <= r.ship_date
  AND f.week_start > r.ship_date - INTERVAL 7 DAY
"""

PUBLISH = """
INSERT INTO copperline.marts.fct_shipping_costs BY NAME
SELECT ?::DATE AS ds, package_id, shipment_id, carrier_code, service_level,
       lane_id, origin_dc, zone, billed_weight_g, card_version,
       rated_cents, billed_cents
FROM copperline.staging.rated_packages
"""

VARIANCE = """
SELECT carrier_code, sum(rated_cents), sum(billed_cents), count(*)
FROM copperline.marts.fct_shipping_costs
WHERE ds = ?::DATE
GROUP BY carrier_code
ORDER BY carrier_code
"""

EXPORT = """
SELECT package_id, shipment_id, lane_id, origin_dc, zone, billed_weight_g,
       card_version, rated_cents, billed_cents
FROM copperline.marts.fct_shipping_costs
WHERE ds = ?::DATE AND carrier_code = ?
ORDER BY package_id
"""


def ensure_tables() -> int:
    """The two tables this DAG owns, on a warehouse that has never held them.

    A fresh deployment and a restore both land here before anything writes, so
    the first run of the day does not have to be the one that noticed.
    """
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS copperline.staging")
        con.execute("CREATE SCHEMA IF NOT EXISTS copperline.marts")
        con.execute("CREATE SCHEMA IF NOT EXISTS copperline.ops")
        con.execute(MART_DDL)
        con.execute(VARIANCE_DDL)
    return 2


def check_card_in_force(ds: str) -> int:
    """Every carrier has exactly one card version in force on the ship date.

    Two versions means the table's own dates overlap, which has happened twice —
    a backdated card and one shipped with no end date. RATE-1's table decides,
    and a run that meets an overlap stops rather than picking one.
    """
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(CARD_IN_FORCE, [ds, ds]).fetchall()
    per_carrier: dict[str, int] = {}
    for _version, carrier in rows:
        per_carrier[carrier] = per_carrier.get(carrier, 0) + 1
    overlapping = sorted(c for c, n in per_carrier.items() if n > 1)
    if overlapping:
        raise ValueError(
            f"{ds}: {', '.join(overlapping)} have more than one card in force. "
            "docs/rate-policy.md RATE-1 decides; correct the table to match it."
        )
    return len(rows)


@lake_task(task_id="rate_packages", lake_config=LAKE_CONFIG, subject="freight")
def rate_packages(ds: str) -> int:
    """Rate the day's packages."""
    with warehouse.connect() as con:
        return int(con.execute(RATE_PACKAGES, [ds, ds]).fetchone()[0])


@lake_task(task_id="park_unrated", lake_config=LAKE_CONFIG, subject="freight")
def park_unrated(ds: str) -> int:
    """Park the packages the card lookup did not reach.

    This is a branch of its own and nothing depends on it. A rejected batch ends
    skipped, and the reject path belongs where a skip costs nothing — the day's
    rated packages still publish, and the parked rows sit in the reject table
    with the reason, waiting for a card.
    """
    with warehouse.connect(read_only=True) as con:
        orphans = con.execute(UNRATED, [ds]).fetchall()
    if orphans:
        columns = ["package_id", "carrier_code", "service_level", "zone",
                   "billed_weight_g", "ship_date"]
        raise Reject(
            f"{len(orphans)} packages have no rate card for {ds}",
            rows=[dict(zip(columns, row)) for row in orphans],
        )
    return 0


def apply_surcharges() -> int:
    """Fuel and accessorials, on the rounded package charge.

    The staging table holds one run's packages, so the week each one takes its
    fuel percentage from comes off its own `ship_date` and no date is passed in.
    """
    with warehouse.connect() as con:
        return int(con.execute(SURCHARGES).fetchone()[0])


def publish_partition(ds: str) -> int:
    """Replace the day's partition of `marts.fct_shipping_costs`."""
    with warehouse.connect() as con:
        con.execute(f"DELETE FROM {warehouse.qualify('marts.fct_shipping_costs')} "
                    "WHERE ds = ?", [ds])
        return int(con.execute(PUBLISH, [ds]).fetchone()[0])


def compare_to_invoice(ds: str) -> int:
    """The day's rated cost against what the carriers billed, per carrier.

    The invoices are not a rate authority and this does not correct them.
    Differences under a dollar a package are written off without investigation —
    a decision from 2023 that nobody has revisited — so what this writes is the
    variance, per carrier, for the person who looks at the trend.
    """
    with warehouse.connect() as con:
        rows = con.execute(VARIANCE, [ds]).fetchall()
        warehouse.delete_insert(
            "ops.freight_variance", "ds", ds,
            [{"ds": ds, "carrier_code": row[0], "rated_cents": row[1],
              "billed_cents": row[2], "packages": row[3]} for row in rows],
            columns=["ds", "carrier_code", "rated_cents", "billed_cents",
                     "packages"],
            con=con,
        )
    return len(rows)


def export_partitions(ds: str) -> list[str]:
    """One published file per carrier, under the name the readers glob for.

    `projects/supply/lib/paths.py` holds the pattern. Two DAGs in this project
    match these files by name rather than by task, so the name is part of the
    contract.
    """
    written = []
    with warehouse.connect(read_only=True) as con:
        for carrier in paths.CARRIERS:
            result = con.execute(EXPORT, [ds, carrier])
            header = [description[0] for description in result.description]
            written.append(str(warehouse.write_partition(
                "marts", f"shipping_costs_{carrier.lower()}", ds,
                header, result.fetchall(),
            )))
    return written


with DAG(
    dag_id="sc_shipping_cost_daily",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["supply", "mart", "freight"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "freight cost build"),
) as dag:
    tables = PythonOperator(
        task_id="ensure_tables", python_callable=ensure_tables)

    cards = PythonOperator(
        task_id="check_card_in_force", python_callable=check_card_in_force,
        op_kwargs={"ds": "{{ ds }}"})

    rated = rate_packages(ds="{{ ds }}")
    parked = park_unrated(ds="{{ ds }}")

    fuel = PythonOperator(
        task_id="apply_surcharges", python_callable=apply_surcharges)
    publish = PythonOperator(
        task_id="publish_partition", python_callable=publish_partition,
        op_kwargs={"ds": "{{ ds }}"})
    variance = PythonOperator(
        task_id="compare_to_invoice", python_callable=compare_to_invoice,
        op_kwargs={"ds": "{{ ds }}"})
    export = PythonOperator(
        task_id="export_partitions", python_callable=export_partitions,
        op_kwargs={"ds": "{{ ds }}"})

    tables >> cards >> rated >> [parked, fuel]
    fuel >> publish >> variance >> export
