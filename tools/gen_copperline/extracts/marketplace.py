"""S2c — Copperline Marketplace: orders, settlement lines and payouts.

Spec 03 section 7 (S2c) and the armed register in section 21. The feed is
written from the **operator's** side, which is what every armed column here
turns on.

  * **`gmv_cents` is the seller's order value and is never Copperline
    revenue.** Copperline's revenue on a marketplace order is the commission
    and the fulfilment fee. A board pack that reads GMV as revenue overstates
    the marketplace by roughly ten times, which is REV-14's first clause and
    C-new-1's second.
  * **`ship_confirmed_at` is the recognition date, not `placed_at`.** The
    seller confirms the shipment one to six days after the order, so a month
    boundary sits between the two on a few per cent of rows every month.
  * **`amount_cents` on a settlement line signs negative for the
    principal.** The principal is money Copperline owes the seller, so from
    the operator's side it is money out. Commission and the fulfilment fee
    sign positive and a refund signs negative against commission.
  * **A refund reduces commission in the period of the refund** and never
    restates the original period, including inside a closed month. The refund
    line posts with the payout run that carried it, three weeks after the one
    that carried the commission, so `posted_at` is the period of the return
    and not the period of the sale.
  * **`entity_code` is the entity the order billed through.** Recognition is
    per legal entity and the currency cannot supply it: CL-IE and CL-DE both
    bill in euro. The column comes from the order's market, through the same
    go-live rule every other feed uses, so a 2024 German order books to CL-US.

DELIVERY. Spec 03 section 2 sends this feed through a paginated API stub,
`include/lib/marketplace_api.py`, whose listings carry `next_page_token` —
the world's enumeration lever. The stub is a platform-phase artifact and is
not written here; this module lands the three tables the stub pages over, so
the data exists and the API can be laid on top of it without regenerating
anything.

`marketplace_order_id` AND `settlement_id` ARE RE-MINTED. The simulation
spells the first as the order id inside eight digits, and Copperline's order
ids run past a hundred million, so it repeats every 500 days; it hashes the
second into twelve digits, which collides on about one line in twenty. An id
that repeats itself is a fault nobody planted, so both are re-minted densely
over the same `MO-########` and `MSET-############` shapes, in a
deterministic order. `order_id` — the true link to `raw.orders` — is the
simulation's throughout.

`loaded_at` ON SETTLEMENT LINES IS RECOMPUTED. The simulation carries the
order's load time onto its settlement lines, which puts some lines in the
warehouse days before the payout that created them. A settlement line is
delivered when the payout runs, so it is landed as the payout date at 07:00
UTC plus the marketplace late tail (`late_tail.marketplace` — 71% same day,
out to day 5, the thickest tail in the world). Order rows keep the load time
the simulation gave them, which already draws from that tail.
"""

from __future__ import annotations

from ..config import Context
from ..streams import draw, lag_days, uniform
from . import _boundary as b

# The hour a weekly payout run writes its lines, 09:00 in Portland. A
# settlement line posts when its payout runs, so that is the timestamp it
# carries, and it is a real hour rather than a date wearing a timestamp's name.
POSTED_HOUR = 17
# The hour the file of those lines reaches the warehouse.
PAYOUT_HOUR = 7


def _keys(ctx: Context) -> None:
    """A dense, unique `MO-########` per seller order."""
    ctx.sql("CREATE SCHEMA IF NOT EXISTS _util")
    ctx.sql("""
        CREATE OR REPLACE TABLE _util._mkt_order_keys AS
        SELECT order_id,
               'MO-' || lpad(row_number() OVER (ORDER BY order_id)::VARCHAR, 8, '0')
                   AS landed_id
        FROM sim_ext.marketplace_orders
    """)


def _orders(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.marketplace_orders (
            marketplace_order_id VARCHAR PRIMARY KEY,
            order_id BIGINT NOT NULL,
            seller_id VARCHAR NOT NULL,
            gmv_cents BIGINT NOT NULL,
            commission_cents BIGINT NOT NULL,
            fulfilment_fee_cents BIGINT NOT NULL,
            commission_rate_bps INTEGER NOT NULL,
            currency_code VARCHAR NOT NULL,
            entity_code VARCHAR NOT NULL,
            placed_at TIMESTAMP NOT NULL,
            ship_confirmed_at TIMESTAMP NOT NULL,
            order_state VARCHAR NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
    """)
    # The seller confirms during the working day, so the recognition clock is
    # a real time of day and not a date wearing a timestamp's name.
    hour = uniform(draw(ctx.seed, "'mkt_confirm_hour'", "m.order_id"), 6, 21)
    minute = uniform(draw(ctx.seed, "'mkt_confirm_min'", "m.order_id"), 0, 59)
    ctx.sql(f"""
        INSERT INTO raw.marketplace_orders
        SELECT
            k.landed_id                                   AS marketplace_order_id,
            m.order_id,
            m.seller_id,
            round(m.gmv_amount * 100)::BIGINT             AS gmv_cents,
            round(m.commission_amount * 100)::BIGINT      AS commission_cents,
            round(m.fulfilment_fee_amount * 100)::BIGINT  AS fulfilment_fee_cents,
            m.commission_rate_bps,
            m.currency_code,
            -- Recognition is per legal entity, so the feed carries the entity
            -- the seller's order billed through. The currency does not decide
            -- it: CL-IE and CL-DE both bill in euro.
            {b.entity_at('ok.market_code', 'CAST(m.ship_confirmed_date AS DATE)')}
                                                          AS entity_code,
            m.placed_at,
            m.ship_confirmed_date::TIMESTAMP
                + INTERVAL 1 HOUR * ({hour})
                + INTERVAL 1 MINUTE * ({minute})          AS ship_confirmed_at,
            m.order_state,
            m.loaded_at
        FROM sim_ext.marketplace_orders m
        JOIN _util._mkt_order_keys k USING (order_id)
        JOIN _util.order_keys ok USING (order_id)
        WHERE m.loaded_at < TIMESTAMP '{ctx.today} 00:00:00'
    """)


def _settlements(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.marketplace_settlements (
            settlement_id VARCHAR PRIMARY KEY,
            payout_id VARCHAR NOT NULL,
            marketplace_order_id VARCHAR NOT NULL,
            order_id BIGINT NOT NULL,
            line_type VARCHAR NOT NULL,
            amount_cents BIGINT NOT NULL,
            currency_code VARCHAR NOT NULL,
            posted_at TIMESTAMP NOT NULL,
            payout_date DATE NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
    """)
    lag = lag_days(
        draw(ctx.seed, "'mkt_line_lag'",
             "s.order_id", "s.line_type", "s.payout_date"),
        ctx.cfg["late_tail"]["marketplace"])
    ctx.sql(f"""
        INSERT INTO raw.marketplace_settlements
        SELECT * FROM (
            SELECT
                'MSET-' || lpad(row_number() OVER (
                    ORDER BY s.order_id, s.line_type, s.payout_date
                )::VARCHAR, 12, '0')                      AS settlement_id,
                s.payout_id,
                k.landed_id                               AS marketplace_order_id,
                s.order_id,
                s.line_type,
                round(s.amount * 100)::BIGINT             AS amount_cents,
                s.currency_code,
                s.payout_date::TIMESTAMP
                    + INTERVAL {POSTED_HOUR} HOUR         AS posted_at,
                s.payout_date,
                s.payout_date::TIMESTAMP + INTERVAL {PAYOUT_HOUR} HOUR
                    + INTERVAL 1 DAY * {lag}              AS line_loaded_at
            FROM sim_ext.marketplace_settlements s
            JOIN _util._mkt_order_keys k USING (order_id)
        )
        WHERE line_loaded_at < TIMESTAMP '{ctx.today} 00:00:00'
    """)


def _payouts(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.marketplace_payouts (
            payout_id VARCHAR PRIMARY KEY,
            seller_id VARCHAR NOT NULL,
            payout_date DATE NOT NULL,
            principal_cents BIGINT NOT NULL,
            commission_cents BIGINT NOT NULL,
            fee_cents BIGINT NOT NULL,
            net_paid_cents BIGINT NOT NULL,
            payout_status VARCHAR NOT NULL
        )
    """)
    # Rolled up from the landed lines, not from the simulation's own payout
    # table, so a payout always ties to the lines that are there. A line that
    # has not arrived yet has no payout to be short of.
    ctx.sql("""
        INSERT INTO raw.marketplace_payouts
        SELECT
            s.payout_id,
            p.seller_id,
            s.payout_date,
            coalesce(sum(s.amount_cents) FILTER (WHERE s.line_type = 'principal'), 0)      AS principal_cents,
            coalesce(sum(s.amount_cents) FILTER (WHERE s.line_type = 'commission'), 0)     AS commission_cents,
            coalesce(sum(s.amount_cents) FILTER (WHERE s.line_type = 'fulfilment_fee'), 0) AS fee_cents,
            (-sum(s.amount_cents))                                                         AS net_paid_cents,
            'paid'                                                                         AS payout_status
        FROM raw.marketplace_settlements s
        JOIN (SELECT DISTINCT payout_id, seller_id
              FROM sim_ext.marketplace_settlements) p USING (payout_id)
        GROUP BY s.payout_id, p.seller_id, s.payout_date
    """)


def build(ctx: Context) -> None:
    b.ensure_keys(ctx)
    _keys(ctx)
    _orders(ctx)
    _settlements(ctx)
    _payouts(ctx)
    ctx.sql("DROP TABLE _util._mkt_order_keys")
