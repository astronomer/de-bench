"""S11 — the parquet orders export (spec 03 section 16).

    landing/orders_export/dt=<ds>/part-000.parquet

The OMS writes this for a second team, so it is a real off-lineage consumer as
well as a source. It lands as files and nothing else: there is no `raw` mirror,
because reading the tree is the task. The tree holds the RET-2 window — 400
live days, ending at the fixture range's last day — and the archive holds the
rest, so the shape change on 2026-01-01 sits inside the live window with about
eight months of the old shape in front of it.

**Two shapes, and the drift is silent.**

    before 2026-01-01   order_id, customer_ref, order_ts, channel, market_code,
                        gross_cents, discount_cents, tax_cents, net_cents,
                        currency_code
    from   2026-01-01   order_id, customer_id, order_ts, channel, market_code,
                        gross_cents, discount_cents, promo_allocation_cents,
                        tax_cents, net_cents, currency_code

`customer_ref` is renamed to `customer_id` and `promo_allocation_cents`
appears, both on the same night. Read with `union_by_name = true` the old
files NULL-fill the new column and the renamed one splits into two
half-populated columns. Nothing errors, which is the whole of W4.

The rename lives inside this export. `raw.orders` keeps `customer_ref`
throughout, so the two feeds disagree about the name of the same value — and
because the old shape spans E2, the old files also carry both id formats.

`promo_allocation_cents` is the header discount spread over the order's lines,
which the OMS started publishing when it started computing it. It is derived
from `sim_sales.order_line_discounts`, not from `raw.promo_applications`: an
extract reads the sim and never another extract.
"""

from __future__ import annotations

from ..config import Context
from . import _boundary as b

# The night the export changed shape. Not an era in timeline.yaml — it is a
# delivery incident, and spec 03 section 16 owns the date.
SCHEMA_CHANGE = "2026-01-01"


def build(ctx: Context) -> None:
    b.ensure_keys(ctx)
    _source(ctx)
    old = b.land(ctx, "orders_export", "part-000.parquet",
                 _query(ctx, before=True), fmt="parquet")
    new = b.land(ctx, "orders_export", "part-000.parquet",
                 _query(ctx, before=False), fmt="parquet")
    days = (ctx.end - b.live_from(ctx)).days + 1
    assert old + new == days, (old, new, days)
    ctx.sql("DROP TABLE _util._orders_export")


def _source(ctx: Context) -> None:
    e2 = b.era_date(ctx, "E2_customer_rekey")
    e4 = b.era_date(ctx, "E4_halcyon_non_usd")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util._orders_export AS
        SELECT k.ds,
               k.raw_order_id                                    AS order_id,
               CASE WHEN k.channel <> 'trade' THEN NULL
                    WHEN k.ds < DATE '{e2}' OR l.order_id IS NOT NULL
                         THEN c.era_ref_before
                    ELSE c.customer_ref END                      AS customer_ref,
               k.event_local                                     AS order_ts,
               k.channel, k.market_code,
               {b.cents('k.subtotal_amount')}                    AS gross_cents,
               {b.cents('k.order_discount_amount')}              AS discount_cents,
               coalesce(a.allocated_cents, 0)                    AS promo_allocation_cents,
               {b.cents('k.tax_amount')}                         AS tax_cents,
               {b.cents('k.grand_total')}                        AS net_cents,
               CASE WHEN k.ds >= DATE '{e4}' THEN k.currency_code END
                                                                 AS currency_code
        FROM _util.order_keys k
        LEFT JOIN _util.customer_keys c ON c.customer_id = k.customer_id
        LEFT JOIN _util.e2_late_orders l ON l.order_id = k.order_id
        LEFT JOIN (
            SELECT ol.order_id, {b.cents('sum(d.discount_amount)')} AS allocated_cents
            FROM sim_sales.order_line_discounts d
            JOIN sim_sales.order_lines ol ON ol.order_line_id = d.order_line_id
            GROUP BY ol.order_id
        ) a ON a.order_id = k.order_id
    """)


def _query(ctx: Context, before: bool) -> str:
    """One era's file shape. The column list is the whole difference."""
    id_column = ("customer_ref" if before else "customer_ref AS customer_id")
    promo = "" if before else "promo_allocation_cents,"
    window = (f"ds < DATE '{SCHEMA_CHANGE}'" if before
              else f"ds >= DATE '{SCHEMA_CHANGE}'")
    return f"""
        SELECT ds AS dt, order_id, {id_column}, order_ts, channel, market_code,
               gross_cents, discount_cents, {promo} tax_cents, net_cents,
               currency_code
        FROM _util._orders_export
        WHERE {window} AND ds >= DATE '{b.live_from(ctx)}'
        ORDER BY ds, order_id
    """
