"""`ops.load_control` — the control table the legacy Pentaho chain reads.

Not an extract. It is the platform's own bookkeeping, landed by the generator
under the same rules and read-only to every task (spec chapter 03 section 1
rule 8).

**What the table is for.** `legacy/pdi/nightly_load.kjb` is an eight-step job
with real variable-passing discipline: a constants transform sets the staging
schema and the watermark column, a control step resolves the load context for
the night from this table, the load steps consume those variables, and a
history-complete step brackets the whole job. One row here is one job's
context for one business date.

**E7, 2026-01-05.** Wave 1 moved nine of the twenty-two nightly jobs to
Airflow. Those nine stop writing rows here on the cutover — their run history
lives in the Airflow deployment from that date, and there is none before it
(`docs/runbooks/orchestrator-migration.md`). The thirteen that did not move
keep writing to the day the range ends. So the table holds twenty-two jobs
before the cutover and thirteen after, and nothing says why.

**The control break, 2026-04-06.** From that date the constants transform
stops producing `watermark_column`, so the control step resolves a context
with no watermark in it and the load steps have nothing to filter on. The
sixth is the last night the job completes. From 2026-04-07 every legacy job
fails. There is no note about it anywhere: the symptom is all there is, and
the thirteen jobs are supply-chain reporting, so no `raw` table loses rows
when they stop.
"""

from __future__ import annotations

import datetime as dt

from .. import streams
from ..config import Context

# The twenty-two nightly jobs the Pentaho estate ran. The nine wave 1 moved
# are the ones `docs/runbooks/orchestrator-migration.md` names; the thirteen
# below them still run, and they are what the control break takes down.
# (job name, stream, cadence in days)
MIGRATED = (
    ("nightly_supplier_master", "supply_chain.suppliers", 1),
    ("nightly_inventory_report", "inventory.balances", 1),
    ("nightly_dc_transfer", "inventory.transfers", 1),
    ("nightly_shrink_weekly", "inventory.counts", 7),
    ("nightly_lane_performance", "logistics.lanes", 1),
    ("nightly_backorder", "sales.backorders", 1),
    ("nightly_dc_capacity", "inventory.capacity", 1),
    ("nightly_scorecard", "supply_chain.scorecard", 7),
    ("nightly_fill_rate", "logistics.fill_rate", 1),
)
SURVIVING = (
    ("nightly_price_change", "product.prices", 1),
    ("nightly_store_sales", "pos.sales_header", 1),
    ("nightly_gl_extract", "finance.journal_lines", 1),
    ("nightly_ar_aging", "finance.ar_invoices", 1),
    ("nightly_vendor_returns", "inventory.rtv", 1),
    ("nightly_markdown", "product.markdowns", 1),
    ("nightly_receipts", "supply_chain.receipts", 1),
    ("nightly_stock_ledger", "inventory.stock_ledger", 1),
    ("nightly_purchase_orders", "supply_chain.purchase_orders", 1),
    ("nightly_store_targets", "store.targets", 7),
    ("nightly_customer_master", "customer.customers", 1),
    ("nightly_promo_calendar", "product.promotions", 1),
    ("nightly_freight_accrual", "logistics.freight", 1),
)

STAGING_SCHEMA = "stg_pdi"
WATERMARK_COLUMN = "loaded_at"

LOAD_ID_BASE = 700_000


def build(ctx: Context) -> None:
    migration = dt.date.fromisoformat(str(ctx.era("E7_orchestrator_migration")["at"]))
    broke = dt.date.fromisoformat(
        str(ctx.era("E7_orchestrator_migration")["control_break"]))

    ctx.sql("""
        CREATE OR REPLACE TABLE ops.load_control (
            load_id BIGINT NOT NULL PRIMARY KEY,
            job_name VARCHAR NOT NULL,
            stream_name VARCHAR NOT NULL,
            business_date DATE NOT NULL,
            staging_schema VARCHAR NOT NULL,
            watermark_column VARCHAR,
            watermark_value TIMESTAMP,
            load_status VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            ended_at TIMESTAMP,
            rows_loaded BIGINT,
            history_complete BOOLEAN NOT NULL
        )
    """)
    jobs = ", ".join(
        f"('{name}', '{stream}', {cadence}, {'true' if migrated else 'false'})"
        for migrated, group in ((True, MIGRATED), (False, SURVIVING))
        for name, stream, cadence in group
    )
    draw = streams.draw(ctx.seed, "'ops.load_control'", "d.ds", "j.job_name")
    ctx.sql(f"""
        INSERT INTO ops.load_control
        WITH j(job_name, stream_name, cadence, migrated) AS (VALUES {jobs}),
        r AS (
            SELECT d.ds AS business_date, j.job_name, j.stream_name, j.migrated,
                   {draw} AS dw
            FROM _util.days d
            CROSS JOIN j
            WHERE (d.ds - DATE '{ctx.start}') % j.cadence = 0
              -- Wave 1's nine jobs write their last row the night before the
              -- cutover. After it their history is the Airflow deployment's,
              -- and it starts on the cutover date.
              AND NOT (j.migrated AND d.ds >= DATE '{migration}')
        ), c AS (
            SELECT r.*,
                   -- The constants transform stops setting the watermark
                   -- column on the sixth. The sixth still completes; every
                   -- night after it fails, because the load steps read a
                   -- variable that is no longer there.
                   r.business_date < DATE '{broke}' AS has_watermark,
                   r.business_date > DATE '{broke}' AS failed
            FROM r
        )
        SELECT {LOAD_ID_BASE} + row_number() OVER (ORDER BY business_date, job_name),
               job_name, stream_name, business_date,
               '{STAGING_SCHEMA}',
               CASE WHEN has_watermark THEN '{WATERMARK_COLUMN}' END,
               CASE WHEN has_watermark
                    THEN business_date::TIMESTAMP + INTERVAL 1 DAY
                         - INTERVAL 1 MINUTE END,
               CASE WHEN failed THEN 'failed' ELSE 'complete' END,
               business_date::TIMESTAMP + INTERVAL 1 DAY
                   + INTERVAL 1 HOUR + INTERVAL 1 MINUTE * (dw % 55),
               CASE WHEN NOT failed
                    THEN business_date::TIMESTAMP + INTERVAL 1 DAY
                         + INTERVAL 1 HOUR + INTERVAL 1 MINUTE * (dw % 55)
                         + INTERVAL 1 MINUTE * (4 + (dw >> 12) % 40) END,
               CASE WHEN failed THEN 0
                    ELSE (400 + (dw >> 24) % 90000)::BIGINT END,
               NOT failed
        FROM c
        ORDER BY business_date, job_name
    """)
