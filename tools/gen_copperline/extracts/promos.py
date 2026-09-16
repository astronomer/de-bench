"""S12 — promotions, gift cards and returns, in the S1 nightly extract.

    raw.promotions          the promotion register, 380 rows
    raw.promo_applications  what each order actually took, header and line
    raw.gift_cards          the OMS gift-card tender module
    raw.gift_card_ledger    issue, redeem, adjust, expire, breakage
    raw.returns             one row per returned line
    landing/oms/dt=<ds>/    promo_applications.csv, gift_card_ledger.csv,
                            returns.csv

`raw.promotions` and `raw.gift_cards` arrive as whole-table extracts and are
mirrored into `raw` only (spec 03 section 1 rule 5): a register of 380 rows is
not re-sent every night.

## What is armed here

**`funded_by`.** A vendor-funded discount is money the supplier gave back, and
the board-pack contract excludes it from net revenue while the finance policy
does not. Same number, two definitions, and C-new-1 has to pick one.

**`applied_seq` against `stack_priority`.** The register says which promotion
should apply first; `applied_seq` says which one did. They disagree, and the
two promotions that share a priority are chosen from the pair that co-occurs
on the most orders, so the tie is reachable rather than theoretical.

**The header discount is not in here twice.** An order-level promotion lands
as one row with `order_line_id` NULL carrying the whole header amount; a
line-level promotion lands per line. Adding both to the line discounts is the
double count the allocation clause exists to catch.

**`return_channel` and `refund_method`.** A pickup line returned by mail makes
channel-level net revenue wrong under naive attribution while the company
total stays identical, and only `original` refunds reach the processor feed —
`gift_card` and `store_credit` look like orphans to PAY-207 unless the policy
is read.

**Breakage.** The tender module writes a breakage entry for every card that
still holds value 24 months after issue, for the value it still holds. It knows
nothing about escheat: that is a legal rule the treasury team owns, and REV-12
applies it at recognition, off `raw.gift_card_jurisdictions.escheat_applies`.
So the feed carries breakage rows a revenue figure must drop, which is what
makes the jurisdiction table load-bearing rather than decorative.
`jurisdiction_code` is `<market>-<nn>`, 66 codes over the nine markets; the
table is hand-authored (rule 7) and must carry exactly those 66.

**`entity_code`.** Struck at the sale, on every entry the card ever has. A card
sold in a market whose local entity had not opened yet was sold by CL-US, and
its redemption and its breakage are CL-US too: the liability sits where it was
struck. REV-12 and the policy's entity table both say so.
"""

from __future__ import annotations

from ..config import Context
from ..streams import pick, uniform
from . import _boundary as b

# Gift cards a day, before the profile divisor: 196,000 over the range.
GIFT_CARDS_PER_DAY = 227
BREAKAGE_MONTHS = 24

# How many gift-card jurisdictions each market holds, 66 in all. A code is
# `<market>-<nn>`; the hand-authored `raw.gift_card_jurisdictions` carries the
# same 66 and decides which of them escheat rather than recognizing breakage.
JURISDICTIONS = {"US": 26, "CA": 8, "DE": 12, "BR": 5, "GB": 4, "MX": 4,
                 "PL": 3, "IE": 2, "ID": 2}


def build(ctx: Context) -> None:
    b.ensure_keys(ctx)
    _promotions(ctx)
    _promo_applications(ctx)
    _stack_priority_tie(ctx)
    _gift_cards(ctx)
    _returns(ctx)
    _land(ctx)


def _promotions(ctx: Context) -> None:
    hp = b.h(ctx, "promotions.shape", "p.promotion_id")
    hf = b.h(ctx, "promotions.funded_by", "p.promotion_id")
    promo_type = (
        "CASE WHEN p.mechanic = 'bogo' THEN 'bogo' "
        f"     WHEN ({hp}) % 100 < 8 THEN 'free_ship' "
        f"     WHEN ({hp}) % 100 < 16 THEN 'threshold' "
        "      WHEN p.mechanic = 'pct_off' THEN 'percent_off' "
        "      ELSE 'amount_off' END"
    )
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.promotions (
            promo_id VARCHAR PRIMARY KEY,
            promo_code VARCHAR NOT NULL,
            promo_type VARCHAR NOT NULL,
            value_bps INTEGER,
            value_cents BIGINT,
            stackable BOOLEAN NOT NULL,
            stack_priority INTEGER NOT NULL,
            applies_to VARCHAR NOT NULL,
            min_order_cents BIGINT,
            starts_at TIMESTAMP NOT NULL,
            ends_at TIMESTAMP NOT NULL,
            market_codes VARCHAR NOT NULL,
            funded_by VARCHAR NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO raw.promotions
        SELECT 'PR-' || lpad(p.promotion_id::VARCHAR, 5, '0'),
               upper(replace(p.name, ' ', '')) || '-'
                   || p.promotion_id::VARCHAR,
               {promo_type},
               CASE WHEN p.discount_pct IS NOT NULL
                    THEN round(p.discount_pct * 10000)::INTEGER END,
               CASE WHEN p.discount_amount IS NOT NULL
                    THEN {b.cents('p.discount_amount')} END,
               ({hp}) % 100 < 64,
               row_number() OVER (ORDER BY p.promotion_id)::INTEGER,
               {pick(f'({hp}) >> 8',
                     [("'sku_list'", 52), ("'category'", 30), ("'order'", 18)])},
               CASE WHEN ({hp}) % 100 < 16
                    THEN {uniform(f'({hp}) >> 20', 5, 40)} * 1000 END,
               p.start_date::TIMESTAMP,
               p.end_date::TIMESTAMP + INTERVAL 23 HOUR + INTERVAL 59 MINUTE,
               CASE WHEN p.start_date < DATE '{b.era_date(ctx, "E4_halcyon_non_usd")}'
                    THEN 'US,CA' ELSE 'US,CA,GB,IE,DE' END,
               {pick(hf, [("'copperline'", 68), ("'vendor'", 24),
                          ("'marketplace'", 8)])}
        FROM sim_product.promotions p
    """)


def _promo_applications(ctx: Context) -> None:
    """One row per promotion applied. The sim attributes a header discount
    across the order's lines without moving the money; the feed re-rolls that
    attribution back to the order, which is where the money sits."""
    hs = b.h(ctx, "promo_applications.seq", "a.order_id", "a.promotion_id")
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.promo_applications (
            application_id VARCHAR PRIMARY KEY,
            order_id VARCHAR NOT NULL,
            order_line_id VARCHAR,
            promo_id VARCHAR NOT NULL,
            discount_cents BIGINT NOT NULL,
            applied_seq INTEGER NOT NULL,
            computed_by VARCHAR NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO raw.promo_applications
        WITH header AS (
            -- The order took a header discount: one row for the order, the
            -- whole amount, no line.
            SELECT l.order_id, NULL::INTEGER AS line_no,
                   min(d.promotion_id) AS promotion_id,
                   sum(d.discount_amount) AS amount
            FROM sim_sales.order_line_discounts d
            JOIN sim_sales.order_lines l ON l.order_line_id = d.order_line_id
            JOIN sim_sales.orders o ON o.order_id = l.order_id
            WHERE o.order_discount_amount > 0
            GROUP BY l.order_id
        ), lines AS (
            SELECT l.order_id, l.line_no, d.promotion_id,
                   d.discount_amount AS amount
            FROM sim_sales.order_line_discounts d
            JOIN sim_sales.order_lines l ON l.order_line_id = d.order_line_id
            JOIN sim_sales.orders o ON o.order_id = l.order_id
            WHERE o.order_discount_amount = 0 AND l.line_discount_amount > 0
        ), a AS (
            SELECT * FROM header UNION ALL SELECT * FROM lines
        ), j AS (
            -- The sim leaves `promotion_id` NULL where no promotion was live
            -- for that channel on that day. The OMS cannot record a discount
            -- against nothing, so those fall back to a promotion from the
            -- register, drawn from the order.
            SELECT k.raw_order_id AS order_id, k.channel, k.ds, a.line_no,
                   'PR-' || lpad(coalesce(a.promotion_id,
                       (SELECT min(promotion_id) FROM sim_product.promotions)
                       + ({b.h(ctx, 'promo_applications.fallback', 'a.order_id')})
                         % (SELECT count(*) FROM sim_product.promotions)
                   )::VARCHAR, 5, '0') AS promo_id,
                   {b.cents('a.amount')} AS discount_cents,
                   -- The line number breaks the tie: two lines of one order
                   -- can take the same promotion, and a row_number with no
                   -- total order is a row_number that moves between runs.
                   row_number() OVER (PARTITION BY a.order_id
                                      ORDER BY ({hs}), a.line_no NULLS FIRST)
                       AS applied_seq
            FROM a JOIN _util.order_keys k ON k.order_id = a.order_id
        )
        SELECT j.order_id || '-A' || lpad(j.applied_seq::VARCHAR, 2, '0'),
               j.order_id,
               CASE WHEN j.line_no IS NOT NULL
                    THEN 'L-' || lpad(j.line_no::VARCHAR, 2, '0') END,
               j.promo_id, j.discount_cents, j.applied_seq,
               CASE WHEN j.channel = 'marketplace' THEN 'marketplace'
                    ELSE 'oms' END
        FROM j
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE _util.promo_application_days AS
        SELECT a.application_id, k.ds
        FROM raw.promo_applications a
        JOIN _util.order_keys k ON k.raw_order_id = a.order_id
    """)


def _stack_priority_tie(ctx: Context) -> None:
    """Two promotions share a `stack_priority`, and the pair is the one that
    co-occurs on the most orders — a tie nobody can reach is not a trap."""
    pair = ctx.sql("""
        SELECT a.promo_id, b.promo_id, count(*) AS n
        FROM raw.promo_applications a
        JOIN raw.promo_applications b
          ON b.order_id = a.order_id AND b.promo_id > a.promo_id
        GROUP BY 1, 2 ORDER BY n DESC, 1, 2 LIMIT 1
    """).fetchone()
    if not pair:
        return
    first, second, _ = pair
    ctx.sql(f"""
        UPDATE raw.promotions SET stack_priority = (
            SELECT stack_priority FROM raw.promotions WHERE promo_id = '{first}')
        WHERE promo_id = '{second}'
    """)


def _gift_cards(ctx: Context) -> None:
    """The OMS's gift-card tender module. The upstream model does not carry
    it — spec 03 section 3 says so — so the cards and their ledger are minted
    here, against the orders that issued and redeemed them."""
    per_day = max(1, ctx.scale(GIFT_CARDS_PER_DAY))
    jurisdictions = ", ".join(
        f"('{market}', {n})" for market, n in JURISDICTIONS.items())
    hc = b.h(ctx, "gift_cards.card", "g.ds", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.gift_cards (
            card_id VARCHAR PRIMARY KEY,
            issued_at TIMESTAMP NOT NULL,
            issued_order_id VARCHAR,
            initial_cents BIGINT NOT NULL,
            currency_code VARCHAR NOT NULL,
            market_code VARCHAR NOT NULL,
            jurisdiction_code VARCHAR NOT NULL,
            expires_at TIMESTAMP,
            status VARCHAR NOT NULL
        )
    """)
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util._gift_card_src AS
        WITH g AS (
            SELECT d.ds, d.day_index,
                   unnest(generate_series(1, {per_day})) AS i
            FROM _util.days d
        ), n AS (
            SELECT g.*, coalesce(o.n, 0) AS orders_that_day, {hc} AS h0
            FROM g
            LEFT JOIN (SELECT ds, count(*) AS n FROM _util.order_keys GROUP BY ds) o
                   USING (ds)
        ), p AS (
            SELECT n.*,
                   CASE WHEN (n.h0) % 100 < 85 AND n.orders_that_day > 0
                        THEN 1 + (n.h0 >> 8) % n.orders_that_day END AS slot
            FROM n
        )
        SELECT p.ds, p.day_index, p.i,
               'GC-' || lpad((p.day_index * 1000 + p.i)::VARCHAR, 8, '0') AS card_id,
               k.raw_order_id AS issued_order_id,
               coalesce(k.market_code, 'US') AS market_code,
               {b.minute("p.ds::TIMESTAMP + INTERVAL 1 MINUTE * "
                         "(540 + (p.h0) % 720)")} AS issued_at,
               {pick('(p.h0) >> 24',
                     [("2500", 26), ("5000", 34), ("10000", 22),
                      ("25000", 12), ("50000", 6)])}::BIGINT AS initial_cents,
               (p.h0 >> 40) AS h
        FROM p
        LEFT JOIN _util.order_keys k ON k.ds = p.ds AND k.slot = p.slot
    """)
    ctx.sql(f"""
        INSERT INTO raw.gift_cards
        SELECT s.card_id, s.issued_at, s.issued_order_id, s.initial_cents,
               CASE s.market_code WHEN 'GB' THEN 'GBP' WHEN 'IE' THEN 'EUR'
                                  WHEN 'DE' THEN 'EUR' WHEN 'PL' THEN 'EUR'
                                  WHEN 'MX' THEN 'MXN' ELSE 'USD' END,
               s.market_code,
               s.market_code || '-' || lpad((1 + (s.h % j.n))::VARCHAR, 2, '0'),
               s.issued_at + INTERVAL 60 MONTH,
               CASE WHEN s.h % 100 < 46 THEN 'redeemed'
                    WHEN s.h % 100 < 52 THEN 'expired' ELSE 'active' END
        FROM _util._gift_card_src s
        JOIN (VALUES {jurisdictions}) AS j(market, n) ON j.market = s.market_code
    """)

    # The ledger. One issue entry per card; a redeem against a later order for
    # the cards that were spent; breakage two years on for the ones that were
    # not, which is REV-12's clause and the reason the escheat jurisdictions
    # have to be readable from the card.
    hr = b.h(ctx, "gift_card_ledger.redeem", "g.card_id")
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.gift_card_ledger (
            entry_id VARCHAR PRIMARY KEY,
            card_id VARCHAR NOT NULL,
            order_id VARCHAR,
            entry_type VARCHAR NOT NULL,
            amount_cents BIGINT NOT NULL,
            occurred_at TIMESTAMP NOT NULL,
            entity_code VARCHAR NOT NULL
        )
    """)
    # The entity that was trading in the card's market on the day the card was
    # sold, on every entry the card ever has. A card sold in Germany in 2024
    # was sold before CL-DE existed, so it and its redemption and its breakage
    # all book to CL-US, which is what the entity table means by a cross-border
    # market with no local entity. The liability is struck once, at the sale.
    entity = b.entity_at("c.market_code", "c.issued_at::DATE")
    ages_out = f"c.issued_at + INTERVAL {BREAKAGE_MONTHS} MONTH"
    ctx.sql(f"""
        INSERT INTO raw.gift_card_ledger
        WITH c AS (
            SELECT g.*, s.h,
                   (g.issued_at + INTERVAL 1 DAY * (7 + ({hr}) % 400)) AS redeem_at
            FROM raw.gift_cards g JOIN _util._gift_card_src s USING (card_id)
        ), j AS (
            SELECT c.*, k.raw_order_id AS redeem_order_id,
                   {b.cents('k.grand_total')} AS order_cents,
                   least(c.initial_cents, {b.cents('k.grand_total')}) AS room
            FROM c LEFT JOIN _util.order_keys k
                          ON c.status = 'redeemed'
                         AND k.ds = c.redeem_at::DATE
                         AND k.slot = 1 + (c.h >> 12) % 64
        ), r AS (
            -- A card is spent against a real order and never for more than
            -- that order still owes, so `gift_card_applied_cents` can never
            -- exceed `grand_total_cents` even where two cards land on one
            -- order: the second covers what the first left.
            SELECT j.*,
                   greatest(0, least(j.room, j.order_cents - coalesce(sum(j.room) OVER (
                       PARTITION BY j.redeem_order_id ORDER BY j.card_id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)))
                       AS redeem_cents
            FROM j
        ), w AS (
            -- What the card still holds on the day it ages out. Breakage is
            -- unredeemed VALUE, so a card spent down to a stub breaks for the
            -- stub; a redemption after the age-out day does not reduce it.
            SELECT r.*,
                   r.initial_cents - CASE
                       WHEN r.status = 'redeemed' AND r.redeem_order_id IS NOT NULL
                            AND r.redeem_at <= r.issued_at
                                + INTERVAL {BREAKAGE_MONTHS} MONTH
                       THEN r.redeem_cents ELSE 0 END AS unredeemed_cents
            FROM r
        )
        SELECT card_id || '-E1', card_id, issued_order_id, 'issue',
               initial_cents, issued_at, {entity} FROM w c
        UNION ALL
        SELECT card_id || '-E2', card_id, redeem_order_id, 'redeem',
               -redeem_cents,
               {b.minute("redeem_at")}, {entity}
        FROM w c WHERE c.status = 'redeemed' AND c.redeem_order_id IS NOT NULL
          AND c.redeem_cents > 0 AND c.redeem_at::DATE <= DATE '{ctx.end}'
        UNION ALL
        SELECT card_id || '-E3', card_id, NULL, 'breakage',
               -unredeemed_cents,
               {ages_out}, {entity}
        FROM w c WHERE c.unredeemed_cents > 0
          AND ({ages_out})::DATE <= DATE '{ctx.end}'
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE _util.gift_card_applied AS
        SELECT k.order_id, sum(-l.amount_cents)::BIGINT AS applied_cents
        FROM raw.gift_card_ledger l
        JOIN _util.order_keys k ON k.raw_order_id = l.order_id
        WHERE l.entry_type = 'redeem'
        GROUP BY k.order_id
    """)
    ctx.sql("DROP TABLE _util._gift_card_src")


def _returns(ctx: Context) -> None:
    hc = b.h(ctx, "returns.channel", "r.return_id")
    hm = b.h(ctx, "returns.refund_method", "r.return_id")
    hd = b.h(ctx, "returns.received", "r.return_id")
    # A return comes back through the channel that sold it four times in five;
    # the rest are the cross-channel population.
    channel = (
        f"CASE WHEN ({hc}) % 100 < 14 THEN "
        f"    {pick(f'({hc}) >> 8', [("'mail'", 40), ("'store'", 45), ("'marketplace'", 15)])} "
        "     WHEN k.channel = 'marketplace' THEN 'marketplace' "
        "     WHEN k.channel = 'store' THEN 'store' ELSE 'mail' END"
    )
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.returns (
            rma_id VARCHAR PRIMARY KEY,
            order_id VARCHAR NOT NULL,
            order_line_id VARCHAR NOT NULL,
            sku VARCHAR NOT NULL,
            qty DECIMAL(12,3) NOT NULL,
            return_reason VARCHAR NOT NULL,
            initiated_at TIMESTAMP NOT NULL,
            received_at TIMESTAMP,
            refund_cents BIGINT NOT NULL,
            restock_flag BOOLEAN NOT NULL,
            return_channel VARCHAR NOT NULL,
            disposition VARCHAR NOT NULL,
            refund_method VARCHAR NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO raw.returns
        WITH r AS (
            SELECT rt.return_id, rt.order_id, rt.requested_at, rt.reason_code,
                   rt.return_status, rl.order_line_id, rl.variant_id, rl.qty,
                   rl.item_condition, rl.restock_flag, rl.refund_amount,
                   row_number() OVER (ORDER BY rt.requested_at, rt.return_id) AS rma_no
            FROM sim_sales.returns rt
            JOIN sim_sales.return_lines rl ON rl.return_id = rt.return_id
        )
        SELECT 'RMA-' || lpad(r.rma_no::VARCHAR, 7, '0'),
               k.raw_order_id,
               'L-' || lpad(l.line_no::VARCHAR, 2, '0'),
               s.sku, r.qty, r.reason_code,
               {b.minute("r.requested_at")},
               CASE WHEN r.return_status IN ('completed', 'approved')
                    THEN {b.minute("r.requested_at + INTERVAL 1 DAY * (2 + "
                                   f"({hd}) % 9)")} END,
               {b.cents('r.refund_amount')},
               r.restock_flag,
               {channel},
               CASE WHEN r.item_condition = 'damaged' THEN 'damage'
                    WHEN r.restock_flag THEN 'restock' ELSE 'rtv' END,
               {pick(hm, [("'original'", 82), ("'gift_card'", 11),
                          ("'store_credit'", 7)])}
        FROM r
        JOIN _util.order_keys k ON k.order_id = r.order_id
        JOIN sim_sales.order_lines l ON l.order_line_id = r.order_line_id
        JOIN _util.sku_keys s ON s.variant_id = r.variant_id
    """)


def _land(ctx: Context) -> None:
    """The side tables ride the S1 nightly extract, so they land in the same
    dated tree. The register and the card table are whole-table extracts and
    are mirrored into `raw` only."""
    live = b.live_from(ctx)
    b.land(ctx, "oms", "promo_applications.csv", f"""
        SELECT d.ds AS dt, a.*
        FROM raw.promo_applications a
        JOIN _util.promo_application_days d USING (application_id)
        WHERE d.ds >= DATE '{live}'
        ORDER BY d.ds, a.application_id
    """)
    b.land(ctx, "oms", "gift_card_ledger.csv", f"""
        SELECT occurred_at::DATE AS dt, *
        FROM raw.gift_card_ledger WHERE occurred_at >= DATE '{live}'
        ORDER BY occurred_at, entry_id
    """)
    b.land(ctx, "oms", "returns.csv", f"""
        SELECT initiated_at::DATE AS dt, *
        FROM raw.returns WHERE initiated_at >= DATE '{live}'
        ORDER BY initiated_at, rma_id
    """)
