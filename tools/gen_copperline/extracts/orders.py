"""S1 — the orders feed out of the Copperline OMS (spec 03 section 6).

Four tables and one dated landing tree:

    raw.orders                one row per order
    raw.order_lines           one row per line, header discount NOT pushed down
    raw.customer_id_map       the E2 crosswalk, sixty of it going nowhere
    raw.order_status_history  the trailing 400 days only, two feeds
    landing/oms/dt=<ds>/      orders.csv and order_lines.csv, the nightly cut

The shipped `order_id` is `ORD-########` derived from (ds, slot), not from the
simulated key — `_boundary` holds the reason and the mapping, and every other
feed that names an order joins it. `customer_ref`, `order_ref`, the two clocks
and the cents conversion all come from there too.

The store channel's line detail lands in `raw.order_lines` under
`source_system = 'pos_replay'` (chapter 03 section 8), which is what
`docs/runbooks/pos-ingestion.md` sends its reader to look for.

## What is armed here, and how it is built

**`customer_ref` carries two id formats in one column (E2).** A trade order
before 2024-11-04 shows `CUST####`, after it `C-######` — and exactly 45
orders dated after the cutover still show the old format, written by the store
integration nobody told. Those 45 are authored, not drawn: a fixed quota per
day over the fourteen days to `tail_until`, so the count is the same at every
profile and on every run. A reader who applies the crosswalk by date rather
than by format gets those 45 wrong and nothing else.

`customer_ref` is null off the trade channel. A consumer order carries
`loyalty_id` instead, and a guest checkout carries neither.

**`event_time_utc` is NULL on store-sourced rows before E6.** The POS learned
UTC on 2025-11-03; before that the only clock a store row has is local. So
`event_time_local` is populated everywhere and `local_order_date` is the
business date the store kept — which is not `event_time_utc::date` for an
evening sale in the Americas, because the local clock runs behind UTC there.

**`currency_code` and `fx_rate_ppm` are NULL before E4** and USD by
construction, so the same column is a trap on one side of that date and
furniture on the other. From the cutover both are stamped: the rate is the
sim's own daily rate for that currency against USD, in parts per million, so
any expected result can be re-derived from the landed row alone.

**`order_ref` is ambiguous.** Seven digits cannot hold the order key, so
`OE-#######` recycles every 50 days of volume and names about seventeen
orders over the range. It is `upstream.external.ORDER_REF`, the spelling
every processor feed uses, and a settlement joined on it lands on one of
those orders. `raw.payment_intents.order_id` is the true bridge (NLO-4).

**`is_test`, `deleted_at`.** 18,000 synthetic orders at full scale that every
naive population includes, and a soft-deleted population that stays visible.
Both scale with the profile divisor; a boundary that exists at one profile and
not another is worse than one that is smaller.
"""

from __future__ import annotations

from ..config import Context
from ..streams import pick
from . import _boundary as b

# The last day of history the status feed keeps. Spec 03 section 19: at full
# grain the table is 6.2M rows for one clause, so the generator writes the
# retention window and nothing older.
STATUS_HISTORY_DAYS = 400

# `sales.orders.order_status` is the application's vocabulary. The feed ships
# the OMS's own five, which is a coarser set, and `returned` is decided by
# whether a return came back rather than by the order row.
STATUS_MAP = {"pending": "placed", "processing": "paid", "shipped": "fulfilled",
              "delivered": "fulfilled", "completed": "fulfilled",
              "cancelled": "cancelled"}


def build(ctx: Context) -> None:
    b.ensure_keys(ctx)
    _e2_late_rows(ctx)
    _orders(ctx)
    _order_lines(ctx)
    _customer_id_map(ctx)
    _status_history(ctx)
    _land(ctx)


def _customer_id_map(ctx: Context) -> None:
    """`raw.customer_id_map` — the E2 crosswalk, and the sixty legacy ids that
    go nowhere.

    The migration wrote one row per account that had a `CUST####` id and lost
    the link on the last sixty: those rows carry the legacy id with
    `customer_id` NULL and a note saying so. `extracts.crm` writes the same
    legacy ids on `raw.customers.legacy_id` from the same ordinals, so the two
    tables agree on which accounts were ever keyed the old way — including the
    sixty, which the crosswalk cannot translate.

    An inner join to this table drops the orphans' pre-E2 orders.
    `docs/runbooks/customer-id-migration.md` says to roll them up under the
    legacy id instead, so dropping them is wrong and so is ignoring them (D7).
    """
    at = b.era_date(ctx, "E2_customer_rekey")
    hm = b.h(ctx, "customer_id_map.at", "c.trade_seq")
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.customer_id_map (
            legacy_id VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            migrated_at TIMESTAMP NOT NULL,
            note VARCHAR
        )
    """)
    ctx.sql(f"""
        INSERT INTO raw.customer_id_map
        SELECT c.legacy_ref,
               CASE WHEN c.is_orphan THEN NULL ELSE c.customer_ref END,
               TIMESTAMP '{at} 00:00:00' + INTERVAL 1 MINUTE * (({hm}) % 1380),
               CASE WHEN c.is_orphan THEN
                   'no current account found; bill under the legacy id' END
        FROM _util.customer_keys c
        WHERE c.legacy_ref IS NOT NULL
    """)
    n, mapped = ctx.sql(
        "SELECT count(*), count(customer_id) FROM raw.customer_id_map").fetchone()
    assert (mapped, n - mapped) == (b.LEGACY_MAPPED, b.LEGACY_ORPHANS), (n, mapped)


def _e2_late_rows(ctx: Context) -> None:
    """The 45 orders after the re-key that still carry the old id format.

    Authored by quota per day, taking the first trade orders of each day in
    key order, so the population is identical at every profile. The build
    refuses to continue if it cannot find all 45: a silently unarmed row is
    the failure mode this whole pass exists to prevent.
    """
    at = b.era_date(ctx, "E2_customer_rekey")
    quota = ", ".join(f"({offset}, {n})" for offset, n in enumerate(b.E2_LATE_QUOTA))
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.e2_late_orders AS
        WITH eligible AS (
            SELECT k.order_id, k.ds,
                   row_number() OVER (PARTITION BY k.ds ORDER BY k.order_id) AS rn
            FROM _util.order_keys k
            WHERE k.channel = 'trade'
              AND k.ds >= DATE '{at}'
              AND k.ds <= DATE '{b.era_date(ctx, "E2_customer_rekey", "tail_until")}'
        )
        SELECT e.order_id, e.ds
        FROM eligible e
        JOIN (VALUES {quota}) AS q(day_offset, quota)
          ON e.ds = DATE '{at}' + INTERVAL 1 DAY * q.day_offset AND e.rn <= q.quota
    """)
    n = ctx.sql("SELECT count(*) FROM _util.e2_late_orders").fetchone()[0]
    if n != b.E2_LATE_ROWS:
        raise ValueError(
            f"the E2 tail wants {b.E2_LATE_ROWS} old-format orders after the "
            f"cutover and the days hold {n}")


def _source_system(ctx: Context) -> str:
    """Which system posted the row, over an `_util.order_keys` alias `k`.

    A store sale is replayed out of the POS batch, so its header and its lines
    both read `pos_replay`. The acquired estate kept posting through
    Northwave's own system until that namespace froze.
    """
    nwv_frozen = b.era_date(ctx, "E3_northwave_acquisition", "nwv_frozen")
    return (
        f"CASE WHEN k.store_acquired_from = 'northwave' "
        f"          AND k.ds <= DATE '{nwv_frozen}' THEN 'nwv' "
        f"     WHEN k.channel = 'store' THEN 'pos_replay' "
        f"     WHEN k.channel = 'marketplace' THEN 'marketplace_sync' "
        f"     ELSE 'oms' END"
    )


def _orders(ctx: Context) -> None:
    e2 = b.era_date(ctx, "E2_customer_rekey")
    e4 = b.era_date(ctx, "E4_halcyon_non_usd")
    e6 = b.era_date(ctx, "E6_utc_cutover")

    status = (f"CASE WHEN r.order_id IS NOT NULL THEN 'returned' ELSE "
              f"{b._case('k.order_status', STATUS_MAP)} END")
    source = _source_system(ctx)
    customer_ref = (
        f"CASE WHEN k.channel <> 'trade' THEN NULL "
        f"     WHEN k.ds < DATE '{e2}' OR l.order_id IS NOT NULL "
        f"          THEN c.era_ref_before "
        f"     ELSE c.customer_ref END"
    )
    loyalty = (f"CASE WHEN k.channel <> 'trade' AND k.customer_id IS NOT NULL "
               f"     THEN 'LOY-' || lpad(k.customer_id::VARCHAR, 7, '0') END")
    # The incremental watermark. It moves for an edit in the minutes after the
    # order and not for a later status change — the status feed carries those
    # — so INC-1 and NLO-2 can measure it against `loaded_at`.
    updated = b.minute(
        "least(k.event_utc + INTERVAL 1 MINUTE * "
        f"({b.h(ctx, 'orders.updated_at', 'k.ds', 'k.slot')} % 15), "
        "date_trunc('day', k.event_utc) + INTERVAL 23 HOUR + INTERVAL 59 MINUTE)")
    deleted = (
        "CASE WHEN d.order_id IS NULL THEN NULL ELSE "
        + b.minute("k.event_utc + INTERVAL 1 DAY * (3 + "
                   f"{b.h(ctx, 'orders.deleted_at', 'k.ds', 'k.slot')} % 28)")
        + " END")

    ctx.sql("""
        CREATE OR REPLACE TABLE raw.orders (
            order_id VARCHAR PRIMARY KEY,
            customer_ref VARCHAR,
            loyalty_id VARCHAR,
            brand VARCHAR NOT NULL,
            channel VARCHAR NOT NULL,
            store_id VARCHAR,
            market_code VARCHAR NOT NULL,
            event_time_utc TIMESTAMP,
            event_time_local TIMESTAMP NOT NULL,
            local_order_date DATE NOT NULL,
            order_status VARCHAR NOT NULL,
            currency_code VARCHAR,
            fx_rate_ppm BIGINT,
            subtotal_cents BIGINT NOT NULL,
            order_discount_cents BIGINT NOT NULL,
            tax_cents BIGINT NOT NULL,
            shipping_cents BIGINT NOT NULL,
            grand_total_cents BIGINT NOT NULL,
            gift_card_applied_cents BIGINT NOT NULL,
            order_ref VARCHAR NOT NULL,
            source_system VARCHAR NOT NULL,
            is_test BOOLEAN NOT NULL,
            deleted_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
    """)
    n_deleted = ctx.scale(b.SOFT_DELETED_ORDERS)
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util._orders_deleted AS
        SELECT order_id FROM _util.order_keys
        QUALIFY row_number() OVER (
            ORDER BY {b.h(ctx, 'orders.soft_delete', 'order_id')}) <= {n_deleted}
    """)
    ctx.sql(f"""
        INSERT INTO raw.orders
        SELECT k.raw_order_id,
               {customer_ref},
               {loyalty},
               CASE WHEN k.store_acquired_from = 'northwave'
                    THEN 'northwave' ELSE 'copperline' END,
               k.channel,
               k.store_ref,
               k.market_code,
               CASE WHEN k.channel = 'store' AND k.ds < DATE '{e6}'
                    THEN NULL ELSE k.event_utc END,
               k.event_local,
               k.ds,
               {status},
               CASE WHEN k.ds >= DATE '{e4}' THEN k.currency_code END,
               CASE WHEN k.ds >= DATE '{e4}'
                    THEN coalesce(round(fx.rate * 1000000)::BIGINT, 1000000) END,
               {b.cents('k.subtotal_amount')},
               {b.cents('k.order_discount_amount')},
               {b.cents('k.tax_amount')},
               {b.cents('k.shipping_amount')},
               {b.cents('k.grand_total')},
               coalesce(g.applied_cents, 0),
               k.order_ref,
               {source},
               k.is_test,
               {deleted},
               {updated},
               {b.minute(b.loaded_at(ctx, 'orders', 'k.event_utc', ('k.ds', 'k.slot')))}
        FROM _util.order_keys k
        LEFT JOIN _util.customer_keys c ON c.customer_id = k.customer_id
        LEFT JOIN _util.e2_late_orders l ON l.order_id = k.order_id
        LEFT JOIN _util._orders_deleted d ON d.order_id = k.order_id
        LEFT JOIN sim_core.exchange_rates fx
               ON fx.rate_date = k.ds AND fx.from_currency = k.currency_code
              AND fx.to_currency = 'USD'
        LEFT JOIN (SELECT DISTINCT order_id FROM sim_sales.returns
                   WHERE return_status IN ('completed', 'approved')) r
               ON r.order_id = k.order_id
        LEFT JOIN {b.gift_card_source(ctx)} g ON g.order_id = k.order_id
    """)
    ctx.sql("DROP TABLE _util._orders_deleted")

    late = ctx.sql(f"""
        SELECT count(*) FROM raw.orders
        WHERE customer_ref LIKE 'CUST%' AND local_order_date >= DATE '{e2}'
    """).fetchone()[0]
    assert late == b.E2_LATE_ROWS, late


def _order_lines(ctx: Context) -> None:
    """One row per line. `line_discount_cents` is line level only: an order
    that took a header discount has zero on every line, and the header amount
    stays on `raw.orders`. Allocating it is the `int` layer's job.

    `source_system` says which feed posted the line. A store sale's lines read
    `pos_replay`: chapter 03 section 8 puts the POS line detail here rather
    than in a table of its own, and `docs/runbooks/pos-ingestion.md` tells the
    reader to look for it under that value."""
    hf = b.h(ctx, "order_lines.fulfillment", "l.order_id", "l.line_no")
    source = _source_system(ctx)
    fulfillment = (
        f"CASE k.channel WHEN 'marketplace' THEN 'marketplace_fbm' "
        f"     WHEN 'store' THEN 'pickup' "
        f"     WHEN 'web' THEN {pick(hf, [("'ship'", 78), ("'pickup'", 22)])} "
        f"     ELSE {pick(hf, [("'ship'", 85), ("'pickup'", 15)])} END"
    )
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.order_lines (
            order_line_id VARCHAR NOT NULL,
            order_id VARCHAR NOT NULL,
            line_no INTEGER NOT NULL,
            sku VARCHAR NOT NULL,
            qty DECIMAL(12,3) NOT NULL,
            unit_price_cents BIGINT NOT NULL,
            unit_cost_cents BIGINT NOT NULL,
            line_discount_cents BIGINT NOT NULL,
            tax_rate_id VARCHAR,
            tax_cents BIGINT NOT NULL,
            line_total_cents BIGINT NOT NULL,
            fulfillment_type VARCHAR NOT NULL,
            line_status VARCHAR NOT NULL,
            source_system VARCHAR NOT NULL,
            PRIMARY KEY (order_id, order_line_id)
        )
    """)
    ctx.sql(f"""
        INSERT INTO raw.order_lines
        SELECT 'L-' || lpad(l.line_no::VARCHAR, 2, '0'),
               k.raw_order_id,
               l.line_no,
               s.sku,
               l.qty,
               {b.cents('l.unit_price')},
               {b.cents('l.unit_cost')},
               {b.cents('l.line_discount_amount')},
               CASE WHEN l.tax_rate_id IS NOT NULL
                    THEN 'TR-' || lpad(l.tax_rate_id::VARCHAR, 4, '0') END,
               {b.cents('l.tax_amount')},
               {b.cents('l.line_total')},
               {fulfillment},
               l.line_status,
               {source}
        FROM sim_sales.order_lines l
        JOIN _util.order_keys k ON k.order_id = l.order_id
        JOIN _util.sku_keys s ON s.variant_id = l.variant_id
    """)


def _status_history(ctx: Context) -> None:
    """One row per status change, for the trailing 400 days only.

    Two feeds write it — the OMS and the POS replay — and the boundary rows of
    that union are LF-1042's second suspect. The path an order took is decided
    by the status it ended on, so the table agrees with `raw.orders` row for
    row.
    """
    ht = b.h(ctx, "order_status_history.at", "u.order_id", "u.i")
    hw = b.h(ctx, "order_status_history.by", "u.order_id", "u.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.order_status_history (
            order_id VARCHAR NOT NULL,
            from_status VARCHAR NOT NULL,
            to_status VARCHAR NOT NULL,
            changed_at TIMESTAMP NOT NULL,
            changed_by VARCHAR NOT NULL,
            feed VARCHAR NOT NULL
        )
    """)
    # The path comes from the same place `raw.orders` got its status — the sim
    # row and the returns table — rather than from `raw.orders` itself, so the
    # rule that an extract reads the sim and never another extract holds even
    # inside one module.
    ctx.sql(f"""
        INSERT INTO raw.order_status_history
        WITH o AS (
            SELECT k.raw_order_id AS order_id, k.channel, k.event_utc,
                   CASE
                       WHEN rt.order_id IS NOT NULL
                            THEN ['placed', 'paid', 'fulfilled', 'returned']
                       WHEN k.order_status = 'cancelled'
                            THEN ['placed', 'paid', 'cancelled']
                       WHEN k.order_status = 'pending' THEN ['placed']
                       WHEN k.order_status = 'processing' THEN ['placed', 'paid']
                       ELSE ['placed', 'paid', 'fulfilled'] END AS path
            FROM _util.order_keys k
            LEFT JOIN (SELECT DISTINCT order_id FROM sim_sales.returns
                       WHERE return_status IN ('completed', 'approved')) rt
                   ON rt.order_id = k.order_id
            WHERE k.ds > DATE '{ctx.end}' - {STATUS_HISTORY_DAYS}
        ), u AS (
            SELECT o.*, unnest(range(1, len(o.path))) AS i FROM o
        )
        SELECT u.order_id,
               u.path[u.i],
               u.path[u.i + 1],
               {b.minute("u.event_utc + INTERVAL 1 HOUR * (u.i - 1) * 8"
                         f" + INTERVAL 1 MINUTE * (({ht}) % 470)")},
               CASE WHEN u.channel = 'store'
                    THEN 'pos_' || lpad((3000 + ({hw}) % 1000)::VARCHAR, 4, '0')
                    ELSE 'oms_batch' END,
               CASE WHEN u.channel = 'store' THEN 'pos_replay' ELSE 'oms' END
        FROM u
    """)


def _land(ctx: Context) -> None:
    """The nightly cut, as files. The whole-table extract for business date D
    lands under `landing/oms/dt=D/`; `loaded_at` on the row says when it
    actually turned up, which is not always that night. The tree holds the
    RET-2 window and `raw` holds the whole history."""
    live = b.live_from(ctx)
    b.land(ctx, "oms", "orders.csv", f"""
        SELECT k.ds AS dt, o.*
        FROM raw.orders o JOIN _util.order_keys k ON k.raw_order_id = o.order_id
        WHERE k.ds >= DATE '{live}'
        ORDER BY k.ds, o.order_id
    """)
    b.land(ctx, "oms", "order_lines.csv", f"""
        SELECT k.ds AS dt, l.*
        FROM raw.order_lines l
        JOIN _util.order_keys k ON k.raw_order_id = l.order_id
        WHERE k.ds >= DATE '{live}'
        ORDER BY k.ds, l.order_id, l.line_no
    """)
