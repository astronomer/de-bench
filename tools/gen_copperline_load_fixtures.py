"""Land the hand-authored fixtures into a built copperline warehouse.

Extract convention 7 (spec 03 section 1) keeps four kinds of file out of the
generator and in the shipped tree: the ERP ledger, the finance workbook, the
reference tables whose exact values a task grades, and `seeds/markets.csv`.
The generator does not write them and holds no reference to them, and
`tests/test_copperline_reference.py` enforces both halves.

Something still has to put them in `raw` before a trial starts, and this is
that something. It sits beside the generator rather than inside it, on the
harness side of the line:

    python tools/gen_copperline_load_fixtures.py \
        --fixtures worlds/copperline/workspace/fixtures \
        --db /opt/copperline/copperline.duckdb \
        --landing /opt/copperline/landing

Two jobs. Every CSV under `fixtures/reference/` lands as its `raw.*` table
with the column types the DAGs that read it declare — `sc_rate_card_intake`
owns the rate-card shape and `sc_shipping_cost_daily` rates against it, so a
sniffed VARCHAR where a BIGINT belongs would break the join and nothing would
say why. And the finance workbook is copied into the landing tree, because
`plat_workbook_inbox` globs `landing/finance/*.csv` and the workbook ships as
source rather than as generated data.

`load(con, fixtures)` is the same work against an open connection. A test that
builds a world by calling the extract modules directly gets the hand-authored
half with one line:

    sys.path.insert(0, str(repo_root() / "tools"))
    from gen_copperline_load_fixtures import DEFAULT_FIXTURES, load

    load(ctx.con, DEFAULT_FIXTURES)
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

#: The copperline world's fixture tree, for a caller that has no path of its own.
DEFAULT_FIXTURES = (Path(__file__).resolve().parents[1]
                    / "worlds" / "copperline" / "workspace" / "fixtures")

# One entry per hand-authored reference table: the file under `fixtures/`, the
# table it lands as, and its columns in file order with the type each reader
# expects. A file with no entry here still lands, with DuckDB sniffing its
# types — see `_sniffed`.
SPECS: dict[str, tuple[str, dict[str, str]]] = {
    "reference/market_config.csv": ("raw.market_config", {
        "market_code": "VARCHAR",
        "country_code": "VARCHAR",
        "market_name": "VARCHAR",
        "billing_currency": "VARCHAR",
        "entity_code": "VARCHAR",
        "tax_regime": "VARCHAR",
        "tax_rate_bps": "INTEGER",
        "price_book_id": "VARCHAR",
        "launched_on": "DATE",
        "is_active": "BOOLEAN",
        "owner": "VARCHAR",
        "notes": "VARCHAR",
    }),
    "reference/entities.csv": ("raw.entities", {
        "entity_code": "VARCHAR",
        "entity_name": "VARCHAR",
        "functional_currency": "VARCHAR",
        "country_code": "VARCHAR",
        "first_traded_on": "DATE",
    }),
    "reference/pay_processor_windows.csv": ("raw.pay_processor_windows", {
        "processor": "VARCHAR",
        "authoritative_from": "DATE",
        "authoritative_to": "DATE",
    }),
    "reference/gift_card_jurisdictions.csv": ("raw.gift_card_jurisdictions", {
        "jurisdiction_code": "VARCHAR",
        "market_code": "VARCHAR",
        "jurisdiction_name": "VARCHAR",
        "subdivision_code": "VARCHAR",
        "escheat_applies": "BOOLEAN",
        "dormancy_months": "INTEGER",
        "escheat_to": "VARCHAR",
        "breakage_allowed": "BOOLEAN",
        "breakage_after_months": "INTEGER",
        "statute_ref": "VARCHAR",
        "effective_from": "DATE",
    }),
    # `sc_rate_card_intake.CARD_COLUMNS` is this shape, in this order.
    "reference/carrier_rate_cards.csv": ("raw.carrier_rate_cards", {
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
    }),
    "reference/carrier_fuel_surcharges.csv": ("raw.carrier_fuel_surcharges", {
        "carrier_code": "VARCHAR",
        "week_start": "DATE",
        "fuel_pct_bps": "INTEGER",
        "landed_on": "DATE",
    }),
    # S5. The ledger is authored under its own rules and lands under the name
    # the close-pack readers use, not under its file name.
    "finance/ledger_monthly.csv": ("raw.finance_ledger", {
        "entity_code": "VARCHAR",
        "fiscal_month": "VARCHAR",
        # The largest value is over INT32, so declaring this one matters: a
        # sniffer that reads the head of the file gets it wrong.
        "recognized_cents": "BIGINT",
        "posted_on": "DATE",
        "preparer": "VARCHAR",
        "memo": "VARCHAR",
    }),
}

#: The workbook is a file, not a table, and it lands under the same relative
#: path in the landing tree. `plat_workbook_inbox` globs `landing/<team>/*.csv`
#: and hands this one to `sc_freight_accrual_workbook`.
WORKBOOK = "finance/logistics_cost_workbook_fy26q1.csv"


def _quote(path: Path) -> str:
    return str(path).replace("'", "''")


def _typed(con, table: str, source: Path, columns: dict[str, str]) -> int:
    types = ", ".join(f"'{name}': '{kind}'" for name, kind in columns.items())
    con.execute(
        f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_csv("
        f"'{_quote(source)}', header = true, columns = {{{types}}})"
    )
    return int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])


def _sniffed(con, table: str, source: Path) -> int:
    con.execute(
        f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_csv("
        f"'{_quote(source)}', header = true)"
    )
    return int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])


def load(con, fixtures: Path) -> dict[str, int]:
    """Land every hand-authored fixture into `raw`. Returns rows per table.

    A file the manifest does not name still lands, as `raw.<stem>` with
    sniffed types, so a new reference table works the day it is committed and
    earns its entry above when a DAG starts caring what type it holds.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    landed: dict[str, int] = {}
    for source in sorted(fixtures.rglob("*.csv")):
        rel = source.relative_to(fixtures).as_posix()
        if rel == WORKBOOK:
            continue
        table, columns = SPECS.get(rel, (f"raw.{source.stem}", {}))
        landed[table] = (_typed(con, table, source, columns) if columns
                         else _sniffed(con, table, source))
    if not landed:
        raise FileNotFoundError(f"no fixtures under {fixtures}")
    return landed


#: Staging views a task's prompt names and a check therefore has to see.
#:
#: dbt materializes `models/shared/staging/` into schema `stg` as views, but a
#: trial is scored against the baked warehouse with no dbt run of its own — the
#: patch carries file changes, not warehouse state, and scoring opens the file
#: read-only. So a prompt that says "read that view" points at nothing and
#: every test touching it dies on a catalog error. Finding 018: five of six
#: c2b6f8 trials failed that way, having followed the prompt exactly.
#:
#: The body is the dbt model's own SELECT with `{{ source('sales','orders') }}`
#: resolved to `raw.orders`. It is copied rather than compiled because the
#: model has no upstream dbt dependency, and a real `dbt build` here would want
#: the whole 141-model graph, network for `dbt deps`, and an environment
#: airflow and dbt-core cannot share. `tests/test_copperline_staging_views.py`
#: asserts this stays column-for-column with the model.
STAGING_VIEWS = {
    "stg.stg_sales__orders": """
        select
            order_id,
            order_ref,
            customer_ref                                as customer_ref,
            loyalty_id,
            brand,
            channel,
            store_id,
            market_code,
            order_status,
            source_system,
            event_time_utc,
            event_time_local,
            local_order_date                            as order_date,
            currency_code,
            fx_rate_ppm,
            cast(subtotal_cents as bigint)              as subtotal_cents,
            cast(order_discount_cents as bigint)        as order_discount_cents,
            cast(tax_cents as bigint)                   as tax_cents,
            cast(shipping_cents as bigint)              as shipping_cents,
            cast(grand_total_cents as bigint)           as grand_total_cents,
            cast(gift_card_applied_cents as bigint)     as gift_card_applied_cents,
            updated_at,
            loaded_at
        from raw.orders
        where not coalesce(is_test, false)
          and deleted_at is null
    """,
}


def create_staging_views(con) -> dict[str, int]:
    """Materialize the staging views a check has to be able to read.

    Returns rows per view. Idempotent — `CREATE OR REPLACE`, so a rebuild over
    an existing warehouse is safe.
    """
    created: dict[str, int] = {}
    for name, body in sorted(STAGING_VIEWS.items()):
        schema = name.split(".", 1)[0]
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        con.execute(f"CREATE OR REPLACE VIEW {name} AS {body}")
        created[name] = int(con.execute(f"SELECT count(*) FROM {name}").fetchone()[0])
    return created


def land_workbook(fixtures: Path, landing: Path) -> Path | None:
    """Copy the finance workbook into the landing tree it is picked up from."""
    source = fixtures / WORKBOOK
    if not source.exists():
        return None
    target = landing / WORKBOOK
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--landing", type=Path)
    args = parser.parse_args(argv)

    import duckdb

    con = duckdb.connect(str(args.db))
    try:
        landed = load(con, args.fixtures)
        views = create_staging_views(con)
    finally:
        con.close()
    for table, rows in sorted(landed.items()):
        print(f"{table}: {rows} rows")
    for view, rows in sorted(views.items()):
        print(f"{view}: {rows} rows (view)")
    if args.landing:
        written = land_workbook(args.fixtures, args.landing)
        if written:
            print(f"workbook: {written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
