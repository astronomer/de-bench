"""The frozen `nwv` namespace: a second DuckDB file beside the warehouse.

`include/lib/warehouse.connect()` attaches this file READ_ONLY as `nwv` and
sets `search_path = 'nwv,copperline'`. Note the order: an UNQUALIFIED table
name resolves against the frozen copy FIRST. That is the trap chapter 05
describes — the acquired reports kept running unqualified while their models
were ported, the port never finished, and the copy stopped refreshing when
the namespace froze on 2025-09-30.

WHAT IS IN IT. One schema, `raw`, holding Northwave's own book as the
integration team loaded it. Twelve tables carry a Copperline `raw` name:

    customers  stores  orders  order_lines  order_status_history  invoices
    inventory_snapshots  wms_movements  pos_sales_daily
    product_categories  payment_terms  fiscal_calendar

Each of those mirrors the Copperline table's columns — the same names, the
same types, and Copperline's vocabulary for every status — so a query written
for Copperline RUNS against the frozen copy and returns Northwave's numbers
instead of an error. Columns Northwave never held are NULL of the right type.
Eight more tables keep Northwave's own names (`accounts`, `departments`,
`classes`, `subclasses`, `products`, `stock_ledger`, `gl_accounts`,
`gl_journal`) so the copy reads as a real book rather than a decoy.

THE CLOCK. `sim_nwv` already ends at `nwv_frozen`, so a copy of it is frozen
by construction. The load stamps end there too: the copy refreshed nightly at
03:20 the morning after a business date, and the last refresh ran on the
night of the freeze, so no timestamp in this file falls after 2025-09-30.

WHERE IT IS WRITTEN. Beside the live warehouse, from the live connection's
own path — the CLI's `--db` and the image bake both give the sibling for
free. It is written directly rather than through the orchestrator's compact
copy, which swaps the main file only. A build that fails the invariant pass
deletes this file with the main one; `build.py` does that with `sibling_of`.

NO ANSWER KEY. `sim_nwv.accounts.pair_role` and `nwv_seq` decide the 300 true
merge pairs and the 60 decoys, so neither leaves this module. `_check`
asserts it.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from ..config import Context
from . import _boundary as b

#: The attach alias while the generator writes. The world attaches it as
#: `nwv`; the name here only has to differ from the live database's.
ALIAS = "nwv_out"

#: The file name `warehouse.northwave_path()` looks for.
FILE_NAME = "northwave.duckdb"

#: The nightly refresh of the copy: 03:20 the morning after the business date.
LOAD_HOUR_MINUTES = 200

#: Tables that share a Copperline `raw` name. Chapter 05 says twelve, and the
#: count is asserted rather than counted by hand.
SHARED_TABLES = (
    "customers",
    "fiscal_calendar",
    "inventory_snapshots",
    "invoices",
    "order_lines",
    "order_status_history",
    "orders",
    "payment_terms",
    "pos_sales_daily",
    "product_categories",
    "stores",
    "wms_movements",
)

#: Tables under Northwave's own names. No Copperline table is called any of
#: these, so an unqualified read of one is unambiguous.
OWN_TABLES = (
    "accounts",
    "classes",
    "departments",
    "gl_accounts",
    "gl_journal",
    "products",
    "stock_ledger",
    "subclasses",
)

#: Northwave's own terms book. Four codes, their own numbering.
PAYMENT_TERMS = (
    ("NWT-15", "Net 15 days from invoice", 15, 0, 0),
    ("NWT-30", "Net 30 days from invoice", 30, 0, 0),
    ("NWT-45", "Net 45 days from invoice", 45, 0, 0),
    ("NWT-2-10", "2 per cent 10 days, net 30", 30, 200, 10),
)

#: Northwave's order vocabulary mapped onto Copperline's. The words have to
#: agree or the trap stops being silent: `WHERE order_status = 'fulfilled'`
#: must return rows from the frozen copy, not none.
ORDER_STATUS = {"closed": "fulfilled", "open": "paid", "cancelled": "cancelled"}
LINE_STATUS = {"closed": "completed", "open": "processing", "cancelled": "cancelled"}


def sibling_of(db: Path | str) -> Path:
    """The frozen copy's path, from the live warehouse's path."""
    return Path(db).with_name(FILE_NAME)


def build(ctx: Context) -> None:
    path = sibling_of(_live_path(ctx))
    for stale in (path, Path(f"{path}.wal")):
        stale.unlink(missing_ok=True)
    ctx.sql(f"ATTACH '{_literal(path)}' AS {ALIAS}")
    try:
        ctx.sql(f"CREATE SCHEMA {ALIAS}.raw")
        _own(ctx)
        _customers(ctx)
        _stores(ctx)
        _orders(ctx)
        _order_lines(ctx)
        _status_history(ctx)
        _invoices(ctx)
        _inventory_snapshots(ctx)
        _wms_movements(ctx)
        _pos_sales_daily(ctx)
        _product_categories(ctx)
        _payment_terms(ctx)
        _fiscal_calendar(ctx)
        _check(ctx)
    finally:
        ctx.sql(f"DETACH {ALIAS}")


# --- where the file goes ---------------------------------------------------

def _live_path(ctx: Context) -> Path:
    """The live warehouse's own path, asked of the live connection.

    The generator is given `--db` and the image bake gives another path
    again, so the sibling is derived here rather than passed down: whatever
    file this build is writing, the frozen copy lands beside it.
    """
    row = ctx.sql(
        "SELECT path FROM duckdb_databases() "
        "WHERE database_name = current_database()"
    ).fetchone()
    if not row or not row[0]:
        raise RuntimeError(
            "the live warehouse has no path, so the frozen nwv copy has "
            "nowhere to go — the generator cannot write an in-memory build"
        )
    return Path(row[0])


def _literal(path: Path) -> str:
    return str(path).replace("'", "''")


# --- the frozen clock ------------------------------------------------------

def _frozen(ctx: Context) -> dt.date:
    return b.era_date(ctx, "E3_northwave_acquisition", "nwv_frozen")


def _last_load(ctx: Context) -> str:
    """The final refresh, on the night of the freeze, as a SQL literal.

    It runs late enough to have carried the freeze date's own rows, so no row
    in the file is stamped as loaded before the event it describes.
    """
    return "TIMESTAMP '{} 23:50:00'".format(_frozen(ctx))


def _loaded_at(ctx: Context, stream: str, date_expr: str, *parts: str) -> str:
    """When the copy saw a row: 03:20 the morning after its business date,
    and never after the last refresh."""
    minutes = b.h(ctx, f"nwv_frozen.{stream}.load", *parts)
    return (
        f"least(({date_expr})::TIMESTAMP + INTERVAL 1 DAY"
        f" + INTERVAL {LOAD_HOUR_MINUTES} MINUTE"
        f" + INTERVAL 1 MINUTE * (({minutes}) % 40), {_last_load(ctx)})"
    )


def _case(column: str, mapping: dict[str, str], default: str) -> str:
    arms = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in mapping.items())
    return f"CASE {column} {arms} ELSE '{default}' END"


def _terms_code(ctx: Context, account_expr: str) -> str:
    """An account's terms code, drawn from the account id.

    Both `customers` and `invoices` need it and they draw it the same way, so
    neither has to read the other's table — an extract reads the sim and never
    another extract, which holds inside one module too.
    """
    h = b.h(ctx, "nwv_frozen.customers.terms", account_expr)
    arms = " ".join(f"WHEN {i} THEN '{code}'"
                    for i, (code, *_rest) in enumerate(PAYMENT_TERMS))
    return f"(CASE ({h}) % {len(PAYMENT_TERMS)} {arms} END)"


# --- Northwave's own tables ------------------------------------------------

def _own(ctx: Context) -> None:
    """Straight copies, minus the two columns that decide the merge answer.

    `pair_role` names the 300 true pairs and the 60 decoys outright and
    `nwv_seq` gives the same answer by ordinal, so `accounts` ships neither.
    """
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.accounts AS
        SELECT nwv_account_id, account_name, primary_contact, contact_email,
               phone, billing_street, billing_city, billing_state,
               billing_postal, country, tax_id, opened_on, status, owner,
               legacy_crm_id
        FROM sim_nwv.accounts ORDER BY nwv_account_id
    """)
    for table, order in (("departments", "dept_seq"), ("classes", "class_seq"),
                         ("subclasses", "subclass_seq"), ("products", "product_seq"),
                         ("stock_ledger", "movement_id"),
                         ("gl_journal", "entry_id, line_no")):
        ctx.sql(f"CREATE TABLE {ALIAS}.raw.{table} AS "
                f"SELECT * FROM sim_nwv.{table} ORDER BY {order}")
    ctx.sql(f"CREATE TABLE {ALIAS}.raw.gl_accounts AS "
            f"SELECT * FROM sim_nwv.gl_accounts ORDER BY account_code")


# --- the twelve shared names -----------------------------------------------

def _customers(ctx: Context) -> None:
    """`raw.customers` over the `NWA-#####` book. 1,400 rows against
    Copperline's 4,000, which is the count the trap is measured by.

    Northwave kept no billing era and no account tier, so both are NULL. It
    did keep terms, and they are its own codes."""
    updated = b.h(ctx, "nwv_frozen.customers.updated", "a.nwv_account_id")
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.customers AS
        SELECT a.nwv_account_id                              AS customer_id,
               a.legacy_crm_id                               AS legacy_id,
               a.account_name,
               a.primary_contact                             AS contact_name,
               a.contact_email                               AS email,
               a.phone,
               a.billing_street                              AS address_line1,
               a.billing_city                                AS city,
               a.billing_state                               AS region_code,
               a.billing_postal                              AS postal_code,
               a.country                                     AS country_code,
               'US'                                          AS market_code,
               a.tax_id,
               a.opened_on                                   AS created_on,
               a.status,
               NULL::VARCHAR                                 AS billing_era,
               NULL::VARCHAR                                 AS tier,
               {_terms_code(ctx, 'a.nwv_account_id')}         AS payment_terms_code,
               NULL::TIMESTAMP                               AS deleted_at,
               greatest(a.opened_on::TIMESTAMP,
                        {_last_load(ctx)} - INTERVAL 1 DAY * (({updated}) % 620))
                                                             AS updated_at,
               'northwave'                                   AS source_brand
        FROM sim_nwv.accounts a
        ORDER BY a.nwv_account_id
    """)


def _stores(ctx: Context) -> None:
    """`raw.stores`, 44 rows. Northwave ran no store history, so every row is
    the current version and `valid_from` is the opening date."""
    fmt = b.h(ctx, "nwv_frozen.stores.format", "s.store_code")
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.stores AS
        SELECT s.store_code                                  AS store_id,
               s.store_name,
               CASE ({fmt}) % 3 WHEN 0 THEN 'yard' WHEN 1 THEN 'branch'
                                ELSE 'depot' END             AS store_format,
               s.region_name                                 AS region_code,
               'US'                                          AS market_code,
               NULL::VARCHAR                                 AS tz_name,
               s.opened_on,
               NULL::DATE                                    AS closed_on,
               NULL::VARCHAR                                 AS acquired_from,
               'open'                                        AS status,
               s.opened_on                                   AS valid_from,
               NULL::DATE                                    AS valid_to,
               TRUE                                          AS is_current
        FROM sim_nwv.stores s
        ORDER BY s.store_code
    """)


def _orders(ctx: Context) -> None:
    """`raw.orders` off Northwave's own order management system.

    Northwave settled everything in USD and stamped local time only, so
    `event_time_utc` is NULL and `fx_rate_ppm` is unity. It ran no gift cards
    and charged no freight on a trade order, so both are zero.
    """
    status = _case("o.order_status", ORDER_STATUS, "placed")
    channel = ("CASE WHEN o.nwv_account_id IS NULL THEN 'store' "
               "ELSE 'trade' END")
    updated = b.h(ctx, "nwv_frozen.orders.updated", "o.order_id")
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.orders AS
        SELECT o.order_no                                    AS order_id,
               o.nwv_account_id                              AS customer_ref,
               NULL::VARCHAR                                 AS loyalty_id,
               'northwave'                                   AS brand,
               {channel}                                     AS channel,
               o.store_code                                  AS store_id,
               'US'                                          AS market_code,
               NULL::TIMESTAMP                               AS event_time_utc,
               o.order_local_ts                              AS event_time_local,
               o.order_date                                  AS local_order_date,
               {status}                                      AS order_status,
               o.currency_code,
               1000000::BIGINT                               AS fx_rate_ppm,
               {b.cents('o.subtotal_amount')}                AS subtotal_cents,
               {b.cents('o.discount_amount')}                AS order_discount_cents,
               {b.cents('o.tax_amount')}                     AS tax_cents,
               0::BIGINT                                     AS shipping_cents,
               {b.cents('o.grand_total')}                    AS grand_total_cents,
               0::BIGINT                                     AS gift_card_applied_cents,
               o.order_no                                    AS order_ref,
               'nwv_oms'                                     AS source_system,
               FALSE                                         AS is_test,
               NULL::TIMESTAMP                               AS deleted_at,
               {b.minute(f"o.order_local_ts + INTERVAL 1 MINUTE * (({updated}) % 90)")}
                                                             AS updated_at,
               {_loaded_at(ctx, 'orders', 'o.order_date', 'o.order_id')}
                                                             AS loaded_at
        FROM sim_nwv.orders o
        ORDER BY o.order_id
    """)


def _order_lines(ctx: Context) -> None:
    """`raw.order_lines`. Northwave discounted at the header only, so every
    line discount is zero, and it held no tax rate table."""
    line_status = _case("o.order_status", LINE_STATUS, "processing")
    fulfil = b.h(ctx, "nwv_frozen.order_lines.fulfilment", "l.order_line_id")
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.order_lines AS
        SELECT 'L-' || lpad(l.line_no::VARCHAR, 2, '0')      AS order_line_id,
               o.order_no                                    AS order_id,
               l.line_no::INTEGER                            AS line_no,
               l.nwv_sku                                     AS sku,
               l.qty,
               {b.cents('l.unit_price')}                     AS unit_price_cents,
               {b.cents('l.unit_cost')}                      AS unit_cost_cents,
               0::BIGINT                                     AS line_discount_cents,
               NULL::VARCHAR                                 AS tax_rate_id,
               round(l.line_total * 0.0725 * 100)::BIGINT    AS tax_cents,
               {b.cents('l.line_total')}                     AS line_total_cents,
               CASE WHEN ({fulfil}) % 100 < 64 THEN 'pickup'
                    ELSE 'ship' END                          AS fulfillment_type,
               {line_status}                                 AS line_status,
               'nwv_oms'                                     AS source_system
        FROM sim_nwv.order_lines l
        JOIN sim_nwv.orders o ON o.order_id = l.order_id
        ORDER BY l.order_id, l.line_no
    """)


def _status_history(ctx: Context) -> None:
    """`raw.order_status_history`. One transition per order — the OMS wrote
    the closing move and nothing else, which is why the frozen copy is
    thinner than the live table at the same grain."""
    at = b.h(ctx, "nwv_frozen.status.at", "o.order_id")
    who = b.h(ctx, "nwv_frozen.status.by", "o.order_id")
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.order_status_history AS
        SELECT o.order_no                                    AS order_id,
               'placed'                                      AS from_status,
               {_case("o.order_status", ORDER_STATUS, "placed")} AS to_status,
               least({b.minute(
                   "o.order_local_ts + INTERVAL 1 HOUR * (2 + ("
                   + at + ") % 30)")}, {_last_load(ctx)})    AS changed_at,
               'nwv_' || lpad((100 + ({who}) % 40)::VARCHAR, 3, '0') AS changed_by,
               'nwv_oms'                                     AS feed
        FROM sim_nwv.orders o
        WHERE o.order_status <> 'open'
        ORDER BY o.order_id
    """)


def _invoices(ctx: Context) -> None:
    """`raw.invoices` — Northwave billed a trade order on the day it closed.

    One invoice per closed account order. It ran one legal entity, one
    currency and no service plans, so `entity_code` is constant and the two
    service dates are NULL.
    """
    terms_days = " ".join(f"WHEN '{code}' THEN {net}"
                          for code, _d, net, *_r in PAYMENT_TERMS)
    settled = b.h(ctx, "nwv_frozen.invoices.settled", "o.order_id")
    lag = b.h(ctx, "nwv_frozen.invoices.lag", "o.order_id")
    total = "(o.subtotal_amount - o.discount_amount + o.tax_amount)"
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.invoices AS
        WITH i AS (
            SELECT o.order_id,
                   o.order_no,
                   o.nwv_account_id,
                   {_terms_code(ctx, 'o.nwv_account_id')}    AS payment_terms_code,
                   least(o.order_date + INTERVAL 1 DAY * (({lag}) % 3),
                         DATE '{_frozen(ctx)}')::DATE        AS invoice_date,
                   o.subtotal_amount - o.discount_amount     AS net_amount,
                   o.tax_amount,
                   {total}                                   AS total_amount,
                   ({settled}) % 100                          AS settled_draw
            FROM sim_nwv.orders o
            WHERE o.order_status = 'closed' AND o.nwv_account_id IS NOT NULL
        )
        SELECT 'NWI-' || lpad((i.order_id % 100000000)::VARCHAR, 8, '0')
                                                             AS invoice_id,
               'NW-INV-' || strftime(i.invoice_date, '%Y%m') || '-'
                   || lpad((i.order_id % 100000)::VARCHAR, 5, '0')
                                                             AS invoice_number,
               i.nwv_account_id                              AS customer_ref,
               i.nwv_account_id                              AS nwv_account_id,
               i.order_no                                    AS order_id,
               'NWV'                                         AS entity_code,
               'US'                                          AS market_code,
               i.invoice_date,
               NULL::DATE                                    AS service_start,
               NULL::DATE                                    AS service_end,
               (i.invoice_date + INTERVAL 1 DAY *
                   (CASE i.payment_terms_code {terms_days} ELSE 30 END))::DATE
                                                             AS due_date,
               i.payment_terms_code,
               'legacy'                                      AS billing_era,
               'USD'                                         AS currency_code,
               1000000::BIGINT                               AS fx_rate_ppm,
               {b.cents('i.net_amount')}                     AS net_cents,
               {b.cents('i.tax_amount')}                     AS tax_cents,
               {b.cents('i.total_amount')}                   AS total_cents,
               CASE WHEN i.settled_draw < 88
                    THEN {b.cents('i.total_amount')} ELSE 0::BIGINT END
                                                             AS settled_cents,
               CASE WHEN i.settled_draw < 88 THEN 'paid'
                    ELSE 'issued' END                        AS invoice_status,
               strftime(i.invoice_date, '%Y-%m')             AS posted_period,
               {_loaded_at(ctx, 'invoices', 'i.invoice_date', 'i.order_id')}
                                                             AS loaded_at
        FROM i
        ORDER BY i.order_id
    """)


def _inventory_snapshots(ctx: Context) -> None:
    """`raw.inventory_snapshots` — the closing position the stock ledger ran
    to, one row per store and SKU on the freeze date.

    Northwave counted stock but never valued it at retail, so
    `retail_value_cents` and `cost_complement_bps` are NULL and the cost
    method is the standard cost the ledger carries.
    """
    counted = b.h(ctx, "nwv_frozen.inventory.counted", "l.store_code", "l.nwv_sku")
    frozen = _frozen(ctx)
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.inventory_snapshots AS
        SELECT DATE '{frozen}'                               AS snapshot_date,
               l.nwv_sku                                     AS sku,
               l.store_code                                  AS location_id,
               any_value(p.dept_code)                        AS dept_code,
               greatest(sum(l.qty_delta), 0)::DECIMAL(12,3)  AS on_hand_units,
               0::DECIMAL(12,3)                              AS reserved_units,
               0::DECIMAL(12,3)                              AS in_transit_units,
               {b.cents('any_value(p.unit_cost)')}           AS unit_cost_cents,
               NULL::BIGINT                                  AS retail_value_cents,
               NULL::INTEGER                                 AS cost_complement_bps,
               (TIMESTAMP '{frozen} 00:00:00'
                    - INTERVAL 1 DAY * (1 + ({counted}) % 89))
                                                             AS last_counted_at,
               {_last_load(ctx)}                             AS loaded_at
        FROM sim_nwv.stock_ledger l
        JOIN sim_nwv.products p ON p.nwv_sku = l.nwv_sku
        GROUP BY l.store_code, l.nwv_sku
        ORDER BY l.nwv_sku, l.store_code
    """)


def _wms_movements(ctx: Context) -> None:
    """`raw.wms_movements` off the stock ledger. Northwave posted a movement
    against a store rather than a warehouse, so one of the two locations is
    the store and the other is the movement's counterparty."""
    at = b.h(ctx, "nwv_frozen.wms.at", "l.movement_id")
    who = b.h(ctx, "nwv_frozen.wms.by", "l.movement_id")
    inbound = "l.movement_type IN ('receipt', 'transfer_in')"
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.wms_movements AS
        SELECT 'NWM-' || lpad(l.movement_id::VARCHAR, 12, '0') AS movement_id,
               {b.minute("l.posted_on::TIMESTAMP + INTERVAL 1 MINUTE * (("
                         + at + ") % 1200)")}                 AS occurred_at,
               l.nwv_sku                                      AS sku,
               CASE WHEN {inbound} THEN 'NWDC-01' ELSE l.store_code END
                                                              AS from_location,
               CASE WHEN {inbound} THEN l.store_code ELSE 'NWOUT' END
                                                              AS to_location,
               abs(l.qty_delta)                               AS qty,
               l.movement_type,
               CASE l.movement_type WHEN 'sale' THEN 'order_line'
                                    WHEN 'receipt' THEN 'receipt_line'
                                    WHEN 'adjustment' THEN 'count_line'
                                    ELSE 'transfer_line' END  AS reference_type,
               'NWR-' || lpad((l.movement_id % 100000000)::VARCHAR, 8, '0')
                                                              AS reference_id,
               'nwv_op_' || lpad((({who}) % 60)::VARCHAR, 2, '0') AS operator_id,
               {_loaded_at(ctx, 'wms', 'l.posted_on', 'l.movement_id')}
                                                              AS loaded_at
        FROM sim_nwv.stock_ledger l
        ORDER BY l.movement_id
    """)


def _pos_sales_daily(ctx: Context) -> None:
    """`raw.pos_sales_daily` — the store day book, rolled up from the orders
    Northwave's own tills wrote. Cancelled orders never reached it."""
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.pos_sales_daily AS
        SELECT o.store_code                                  AS store_id,
               o.order_date                                  AS business_date,
               count(*)::INTEGER                             AS txn_count,
               {b.cents('sum(o.subtotal_amount)')}           AS gross_cents,
               {b.cents('sum(o.discount_amount)')}           AS discount_cents,
               {b.cents('sum(o.tax_amount)')}                AS tax_cents,
               {b.cents('sum(o.subtotal_amount - o.discount_amount)')}
                                                             AS net_cents
        FROM sim_nwv.orders o
        WHERE o.order_status <> 'cancelled'
        GROUP BY o.store_code, o.order_date
        ORDER BY o.store_code, o.order_date
    """)


def _product_categories(ctx: Context) -> None:
    """`raw.product_categories` — Northwave's three levels flattened into the
    Copperline shape. This is the table the integration team built by hand,
    and it is why the shared names exist at all: they loaded the acquired
    book into the names the reports already used."""
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.product_categories AS
        SELECT d.dept_code                                   AS category_id,
               NULL::VARCHAR                                 AS parent_id,
               d.dept_name                                   AS name,
               d.dept_code,
               DATE '{ctx.start}'                            AS valid_from,
               NULL::DATE                                    AS valid_to
        FROM sim_nwv.departments d
        UNION ALL
        SELECT c.class_code, c.dept_code, c.class_name, c.dept_code,
               DATE '{ctx.start}', NULL::DATE
        FROM sim_nwv.classes c
        UNION ALL
        SELECT s.subclass_code, s.class_code, s.subclass_name, s.dept_code,
               DATE '{ctx.start}', NULL::DATE
        FROM sim_nwv.subclasses s
        ORDER BY 1
    """)


def _payment_terms(ctx: Context) -> None:
    """`raw.payment_terms` — four codes, Northwave's own numbering."""
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.payment_terms (
            payment_terms_code VARCHAR PRIMARY KEY,
            description VARCHAR NOT NULL,
            net_days INTEGER NOT NULL,
            discount_pct_bps INTEGER NOT NULL,
            discount_days INTEGER NOT NULL
        )
    """)
    ctx.con.executemany(
        f"INSERT INTO {ALIAS}.raw.payment_terms VALUES (?,?,?,?,?)",
        [list(row) for row in PAYMENT_TERMS],
    )


def _fiscal_calendar(ctx: Context) -> None:
    """`raw.fiscal_calendar` — calendar months, not Copperline's 4-5-4.

    Northwave closed on the calendar month; `sim_nwv.gl_journal` already
    posts that way. So the frozen copy answers "what period is this date in"
    with a month number, and a period roll-up written for Copperline that
    forgets to qualify gets a month instead of a 4-5-4 period. The copy runs
    from the year before the fixture range to the end of the freeze year,
    which is as far as the calendar was ever built.
    """
    first = dt.date(ctx.start.year - 1, 1, 1)
    last = dt.date(_frozen(ctx).year, 12, 31)
    ctx.sql(f"""
        CREATE TABLE {ALIAS}.raw.fiscal_calendar AS
        WITH d AS (
            SELECT range::DATE AS cal_date
            FROM range(DATE '{first}', DATE '{last}' + INTERVAL 1 DAY,
                       INTERVAL 1 DAY)
        )
        SELECT cal_date,
               'FY' || year(cal_date)::VARCHAR                AS fiscal_year,
               quarter(cal_date)::TINYINT                     AS fiscal_quarter,
               month(cal_date)::TINYINT                       AS fiscal_period,
               week(cal_date)::TINYINT                        AS fiscal_week,
               date_trunc('week', cal_date)::DATE             AS week_start,
               (date_trunc('week', cal_date) + INTERVAL 6 DAY)::DATE AS week_end,
               isodow(cal_date)::TINYINT                      AS day_of_fiscal_week,
               CASE WHEN cal_date >= DATE '{first}' + INTERVAL 1 YEAR
                    THEN (cal_date - INTERVAL 364 DAY)::DATE END AS comp_date_ly,
               CASE WHEN cal_date >= DATE '{first}' + INTERVAL 1 YEAR
                    THEN week(cal_date - INTERVAL 364 DAY)::TINYINT END
                                                              AS comp_week_ly,
               FALSE                                          AS is_53rd_week
        FROM d ORDER BY cal_date
    """)


# --- the refuse-to-write pass for this file --------------------------------

def _check(ctx: Context) -> None:
    """Four things, each of which would break the trap or leak the key: the
    table list, the answer-key columns, the shared column shape, and the
    frozen clock."""
    shipped = tuple(sorted(r[0] for r in ctx.sql(
        f"SELECT table_name FROM duckdb_tables() "
        f"WHERE database_name = '{ALIAS}' AND schema_name = 'raw' ORDER BY 1"
    ).fetchall()))
    want = tuple(sorted(SHARED_TABLES + OWN_TABLES))
    assert shipped == want, (shipped, want)
    assert len(SHARED_TABLES) == 12, len(SHARED_TABLES)

    leaked = ctx.sql(
        "SELECT table_name, column_name FROM duckdb_columns() "
        f"WHERE database_name = '{ALIAS}' "
        "AND column_name IN ('pair_role', 'nwv_seq', 'trade_seq')"
    ).fetchall()
    assert not leaked, leaked

    # Every shared name must hold the Copperline column shape, or a query
    # written for Copperline raises instead of quietly reading this file.
    live_db = ctx.sql("SELECT current_database()").fetchone()[0]
    for table in SHARED_TABLES:
        shape = _columns(ctx, ALIAS, table)
        live = _columns(ctx, live_db, table)
        assert shape, table
        missing = {c: t for c, t in shape.items() if live.get(c) != t}
        assert not missing, (table, missing, {k: live.get(k) for k in missing})

    # The clock. Nothing in the file may fall after the freeze.
    frozen = _frozen(ctx)
    for table, column in ctx.sql(
        "SELECT table_name, column_name FROM duckdb_columns() "
        f"WHERE database_name = '{ALIAS}' AND schema_name = 'raw' "
        "AND data_type = 'TIMESTAMP' ORDER BY 1, 2"
    ).fetchall():
        late = ctx.sql(
            f'SELECT count(*) FROM {ALIAS}.raw."{table}" '
            f'WHERE "{column}"::DATE > DATE \'{frozen}\''
        ).fetchone()[0]
        assert not late, f"{table}.{column} has {late} rows after the freeze"


def _columns(ctx: Context, database: str, table: str) -> dict[str, str]:
    """One `raw` table's column names and types, in either database."""
    return dict(ctx.sql(
        "SELECT column_name, data_type FROM duckdb_columns() "
        f"WHERE database_name = '{database}' AND schema_name = 'raw' "
        f"AND table_name = '{table}'"
    ).fetchall())
