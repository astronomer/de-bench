"""Rebuild the `raw` schema from the committed fixtures.

Produces every landed table under `raw`, from the CSV files in `fixtures/raw/`.
It is what a fresh deployment runs first and what a restore runs after the
archive comes back. It publishes the `raw` asset, which is what the staging
build waits on.

It is a rebuild, not a load. Each table is replaced whole, so running it twice
leaves one copy — but it also throws away anything an intake landed since the
fixtures were cut, which is why it runs at 03:00, before the day's work, and
never in the middle of one.

Owned by data-platform. When this stops, nothing downstream has a source.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, dag

from include.lib import warehouse
from include.lib.notify import notify
from include.lib.pipeline import lake_task

#: What the staging build waits on.
RAW_READY = Asset("duckdb://warehouse/raw")

#: Where the committed fixtures sit, one CSV per landed table.
FIXTURES = "fixtures/raw"

#: The schemas this DAG owns. `ops` and `audit` are created here because the
#: first thing that writes them should not have to.
SCHEMAS = ("raw", "ops", "audit")

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("platform", "raw rebuild failed; nothing downstream has a source"),
}


@dag(
    dag_id="plat_bootstrap_raw",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "raw", "bootstrap"],
    doc_md=__doc__,
)
def plat_bootstrap_raw():
    @lake_task
    def ensure_schemas() -> list[str]:
        """Create the schemas the rest of this DAG writes into."""
        with warehouse.connect() as con:
            for schema in SCHEMAS:
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify(schema)}")
        return list(SCHEMAS)

    @lake_task
    def load_calendars() -> dict[str, int]:
        """The two calendars. Everything else dates itself against them, so
        they land first and a failure here stops the run."""
        return _rebuild(["fiscal_calendar", "market_calendar",
                         "finance_close_calendar", "entities"])

    @lake_task
    def load_dimensions() -> dict[str, int]:
        """The reference books: stores, products, customers, suppliers,
        carriers and the acquisition crosswalk."""
        return _rebuild(["stores", "products", "product_categories", "customers",
                         "nwv_accounts", "customer_id_map", "suppliers",
                         "carriers", "market_config"])

    @lake_task
    def load_orders() -> dict[str, int]:
        """Order headers, across all four channels."""
        return _rebuild(["orders", "marketplace_orders", "pos_sales_header",
                         "pos_sales_daily"])

    @lake_task
    def load_order_lines() -> dict[str, int]:
        """Line grain, returns and refunds."""
        return _rebuild(["order_lines", "returns", "refunds", "promotions"])

    @lake_task
    def load_payments() -> dict[str, int]:
        """Both processors, the intents that bridge them, and the ledger."""
        return _rebuild(["payment_intents", "pay_halcyon_settlements",
                         "pay_meridian_settlements", "pay_processor_windows",
                         "disputes", "finance_ledger", "invoices", "invoice_lines",
                         "credit_memos", "plan_lines", "fx_rates"])

    @lake_task
    def load_inventory() -> dict[str, int]:
        """On-hand, movements and the purchase-order book."""
        return _rebuild(["inventory_snapshots", "inventory_movements",
                         "purchase_orders", "purchase_order_lines",
                         "goods_receipts", "stock_counts"])

    @lake_task
    def load_carriers() -> dict[str, int]:
        """Scans, invoices and the rate cards behind the freight cost."""
        return _rebuild(["carrier_scans", "carrier_invoices", "carrier_rate_cards",
                         "shipments"])

    @lake_task
    def load_web_events() -> dict[str, int]:
        """The clickstream and the marketing feeds. The widest tables here."""
        return _rebuild(["web_events", "marketing_spend", "email_events",
                         "comp_prices"])

    @lake_task
    def verify(loaded: list[dict[str, int]]) -> dict[str, int]:
        """Every table named above exists and holds rows.

        An empty table is the failure worth catching: the CSV was there, the
        load reported success, and the file had a header and nothing under it.
        """
        counts = {table: rows for group in loaded for table, rows in group.items()}
        empty = sorted(table for table, rows in counts.items() if not rows)
        if empty:
            raise RuntimeError(f"landed with no rows: {', '.join(empty)}")
        return counts

    schemas = ensure_schemas()
    groups = [
        load_calendars(),
        load_dimensions(),
        load_orders(),
        load_order_lines(),
        load_payments(),
        load_inventory(),
        load_carriers(),
        load_web_events(),
    ]
    schemas >> groups
    published = EmptyOperator(task_id="publish", outlets=[RAW_READY])
    verify(groups) >> published


def _rebuild(tables: list[str]) -> dict[str, int]:
    """Replace each table from `fixtures/raw/<table>.csv`, and count it.

    One transaction per table rather than one for the group: a fixture that is
    missing or malformed takes its own table down and leaves the others
    landed, which is what makes a partial rebuild worth re-running.
    """
    root = warehouse.workspace_root() / FIXTURES
    counts: dict[str, int] = {}
    with warehouse.connect() as con:
        for table in tables:
            source = root / f"{table}.csv"
            if not source.exists():
                raise FileNotFoundError(f"no fixture for raw.{table} at {source}")
            target = warehouse.qualify(f"raw.{table}")
            con.execute(
                f"CREATE OR REPLACE TABLE {target} AS "
                f"SELECT * FROM read_csv('{source}', header = true, union_by_name = true)"
            )
            counts[table] = con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]
    return counts


plat_bootstrap_raw()
