"""`sim_ext` — the systems outside Copperline, and the seam they make.

Four kinds of system sit outside the twelve internal schemas (spec 02
section 17): the two card processors, Copperline's own marketplace
settlement platform, and the advertising and e-mail platforms. They are
generated here, apart from the internal model, because that separation is
what puts a real seam between two sources of record — the seam every
reconciliation task grades.

BUILD ORDER. **This module runs after `sales`.** It settles what sales
charged, so `sim_sales.payments` must already exist when it runs. In
`upstream.ORDER` it comes last.

THE SEAM. A processor shares exactly one thing with `sales.payments`: the
`gateway_reference`. There is no foreign key, no settled amount, no fee and
no payout date on Copperline's side, and none of the tables here carry
Copperline's `order_id` — the one exception is the marketplace, whose feed
is order-facing by design, and `meridian_intents`, which is the OMS's own
payment-attempt log rather than the processor's. Everything else about a
settlement lives only out here.

The populations disagree with the internal tables on purpose:

  * **Halcyon Payments**, the legacy processor. Settles from before the
    fixture range to 2025-09-30 and then stops. Authoritative through
    2025-08-31; September is a shadow month at about 60% of its volume, sent
    to prove nothing had been dropped. Non-USD volume appears only after E4
    (2025-04-07) and is about 30% of the feed after that date.
  * **Meridian Pay**, the current processor. Live from 2025-07-01, so July
    and August are Meridian's own shadow traffic and the whole quarter is
    double-fed. It recycles `order_ref` across retry attempts, so joining it
    straight to Copperline's order reference matches most rows and is wrong;
    the bridge through `meridian_intents` is the answer. It restates
    settlements up to thirty days back, and it soft-deletes.
  * **Copperline Marketplace.** Seller-facing, not order-facing: a
    remittance is one seller for one period, netting commission, refunds and
    adjustments across many orders. The principal is money out and signs
    negative from the operator's side.
  * **Beacon Ads, Tessera Social, Solstice Search and Larkspur.** Each
    reports its own spend and its own attributed conversions, on its own
    calendar and in the market's currency. They never tie to
    `customer.marketing_campaigns`, and neither side is wrong.

NO ERAS ARE APPLIED HERE. The delivery mess — integer cents, the era-shaped
id formats, the NULL currency column, the two-decimal strings — belongs to
the extract boundary. What this module owns is the era *windows*: which
system was sending anything at all on a given date, and in what currency.

WHAT IT ASSUMES OF `sim_sales`. One view, `sim_ext._card_payments`, is the
only place that reads the sales model, so a rename upstream is a one-line
fix here. It expects:

    sim_sales.orders(order_id, channel_id, order_datetime, currency_code,
                     customer_id, grand_total)
    sim_sales.payments(payment_id, order_id, payment_method_id, amount,
                       currency_code, status, authorized_at, captured_at,
                       gateway_reference)
    sim_sales.payment_methods(payment_method_id, method_type, provider,
                              settlement_days, fee_pct)
    sim_store.channels(channel_id, channel_type)
"""

from __future__ import annotations

import datetime as dt

from ..config import Context
from ..streams import draw, lag_days, uniform

# The order reference the processors and the OMS both spell. Copperline's
# side spells it the same way; if the sales module changes this rule it must
# change here too, or the armed NLO-4 join stops matching at all.
ORDER_REF = "'OE-' || lpad(({order_id} % 10000000)::VARCHAR, 7, '0')"

# Halcyon's merchant accounts. The merchant region is what decides a
# settlement's currency, which is why the feed never needed a currency column
# until Copperline started selling outside the United States.
MERCHANT_ACCOUNTS = (
    ("HAL-US-0117", "US", "USD"), ("HAL-US-0118", "US", "USD"),
    ("HAL-CA-0204", "CA", "USD"), ("HAL-GB-0331", "GB", "GBP"),
    ("HAL-IE-0332", "IE", "EUR"), ("HAL-DE-0333", "DE", "EUR"),
)

# Three platforms report spend. Larkspur is the e-mail platform: it reports
# events, never a daily spend row, which is why it holds campaigns here but
# never appears in `ads_spend_daily`.
AD_PLATFORMS = (("beacon", "Beacon Ads", True), ("tessera", "Tessera Social", True),
                ("solstice", "Solstice Search", True), ("larkspur", "Larkspur", False))
CAMPAIGNS_PER_PLATFORM = 40
AD_MARKETS = (("US", "USD"), ("CA", "USD"), ("GB", "GBP"),
              ("IE", "EUR"), ("DE", "EUR"))
MARKETPLACE_SELLERS = 180


def _card_payments(ctx: Context) -> None:
    """The one view that reads the sales model."""
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_ext")
    ctx.sql("""
        CREATE OR REPLACE VIEW sim_ext._card_payments AS
        SELECT
            p.payment_id,
            p.order_id,
            p.gateway_reference,
            p.amount,
            p.currency_code,
            p.captured_at,
            date_trunc('minute', p.captured_at)          AS captured_minute,
            p.captured_at::DATE                          AS captured_date,
            (p.captured_at + INTERVAL 1 DAY * pm.settlement_days)::DATE AS payout_date,
            pm.fee_pct,
            o.channel_id,
            ch.channel_type
        FROM sim_sales.payments p
        JOIN sim_sales.orders o USING (order_id)
        JOIN sim_sales.payment_methods pm USING (payment_method_id)
        JOIN sim_store.channels ch ON ch.channel_id = o.channel_id
        WHERE pm.method_type = 'card'
          AND p.status = 'captured'
          AND p.captured_at IS NOT NULL
    """)


def _halcyon(ctx: Context) -> None:
    """`sim_ext.halcyon_settlements` — the legacy processor.

    One row per settled transaction. No event grain and no restatement
    mechanism: the older system reissued the whole file instead.

    Two windows are enforced here rather than left to the sales module, so
    the invariant holds whatever sales generates. Nothing settles after the
    feed stops, and nothing settles in a currency other than USD before E4 —
    before that date every order was USD by construction anyway, so the
    guard is a safety net, not a lie.
    """
    s = ctx.seed
    e4 = dt.date.fromisoformat(str(ctx.era("E4_halcyon_non_usd")["at"]))
    e5 = ctx.era("E5_processor_overlap")
    auth_until = dt.date.fromisoformat(str(e5["authoritative_until"]))
    stops = dt.date.fromisoformat(str(e5["to"]))

    ctx.sql("CREATE OR REPLACE TABLE sim_ext.halcyon_merchant_accounts "
            "(merchant_acct VARCHAR, market_code VARCHAR, home_currency VARCHAR)")
    ctx.con.executemany(
        "INSERT INTO sim_ext.halcyon_merchant_accounts VALUES (?,?,?)",
        list(MERCHANT_ACCOUNTS))

    d_acct = draw(s, "'halcyon_acct'", "src.captured_date", "src.gateway_reference")
    d_status = draw(s, "'halcyon_status'", "src.captured_date", "src.gateway_reference")
    d_shadow = draw(s, "'halcyon_shadow'", "c.captured_date", "c.gateway_reference")
    d_batch = draw(s, "'halcyon_batch'", "src.captured_date", "src.gateway_reference")
    d_file = draw(s, "'halcyon_file'", "src.captured_date", "src.gateway_reference")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ext.halcyon_settlements AS
        WITH src AS (
            SELECT c.*,
                   CASE WHEN c.captured_date < DATE '{e4}' THEN 'USD'
                        ELSE c.currency_code END AS settlement_currency
            FROM sim_ext._card_payments c
            WHERE c.channel_type <> 'marketplace'
              AND c.captured_date <= DATE '{stops}'
              -- September is the shadow month: Meridian is authoritative and
              -- Halcyon sends a thinner file to prove nothing was dropped.
              AND (c.captured_date <= DATE '{auth_until}' OR ({shadow}) % 100 < 60)
        )
        SELECT
            'HX-' || lpad((({batch}) % 100000000)::VARCHAR, 8, '0')  AS txn_id,
            {order_ref}                                             AS merchant_ref,
            src.gateway_reference,
            ma.merchant_acct,
            ma.market_code,
            CASE ({status}) % 1000 WHEN 0 THEN 'PENDING'
                 WHEN 1 THEN 'REVERSED' WHEN 2 THEN 'REVERSED'
                 ELSE 'SETTLED' END                                 AS status,
            src.amount::DECIMAL(18,4)                               AS settled_amount,
            src.settlement_currency,
            -- Merchant-local, no offset and no zone column: that is the whole
            -- of what this feed knows about time.
            (src.captured_minute - INTERVAL 1 HOUR *
                CASE ma.market_code WHEN 'GB' THEN -8 WHEN 'IE' THEN -8
                                    WHEN 'DE' THEN -9 ELSE 0 END)   AS txn_local_ts,
            src.payout_date                                         AS settled_on,
            (src.payout_date + INTERVAL 1 DAY * (({file}) % 3))::DATE AS file_date,
            'HB-' || strftime(src.payout_date, '%Y%m%d') || '-'
                  || ma.merchant_acct                               AS batch_id
        FROM src
        -- The merchant account decides the currency, so it is picked from the
        -- accounts that bill in the settlement's own currency. Anything
        -- Halcyon never learned to bill bills out of the US accounts.
        JOIN (SELECT *,
                     row_number() OVER (PARTITION BY home_currency
                                        ORDER BY merchant_acct) - 1 AS rk,
                     count(*)    OVER (PARTITION BY home_currency)  AS in_ccy
              FROM sim_ext.halcyon_merchant_accounts) ma
          ON ma.home_currency = CASE WHEN src.settlement_currency IN ('GBP', 'EUR')
                                     THEN src.settlement_currency ELSE 'USD' END
         AND ma.rk = ({acct}) % ma.in_ccy
    """.format(e4=e4, stops=stops, auth_until=auth_until, shadow=d_shadow,
               batch=d_batch, status=d_status, file=d_file, acct=d_acct,
               order_ref=ORDER_REF.format(order_id="src.order_id")))


def _meridian(ctx: Context) -> None:
    """`sim_ext.meridian_intents` and `sim_ext.meridian_events`.

    The intents table is the OMS's payment-attempt log, which is why it is
    the only table out here that carries `order_id`. One order can hold two
    or three attempts and **every attempt reuses one `order_ref`**, so
    joining a settlement to an order on that reference matches 96% of rows
    and is wrong about which attempt paid. The bridge is the answer.

    The events table is the processor's own. It keys on `event_id`, carries
    the `gateway_reference` and nothing else Copperline holds, restates up
    to thirty days back, and soft-deletes.
    """
    s = ctx.seed
    e5_from = dt.date.fromisoformat(str(ctx.era("E5_processor_overlap")["from"]))
    tail = ctx.cfg["late_tail"]["default"]

    d_attempts = draw(s, "'mp_attempts'", "c.captured_date", "c.gateway_reference")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ext.meridian_intents AS
        SELECT
            'PI-' || lpad(((c.order_id * 10 + a.attempt_no) % 100000000)::VARCHAR, 8, '0') AS intent_id,
            c.order_id,
            {order_ref}                                   AS order_ref,
            a.attempt_no,
            date_trunc('minute', c.captured_at
                - INTERVAL 1 MINUTE * (a.n_attempts - a.attempt_no) * 7) AS created_at,
            CASE WHEN a.attempt_no = a.n_attempts THEN 'succeeded'
                 WHEN ({outcome}) % 3 = 0 THEN 'abandoned'
                 ELSE 'failed' END                        AS outcome,
            c.gateway_reference
        FROM sim_ext._card_payments c
        CROSS JOIN LATERAL (
            SELECT g.attempt_no, x.n_attempts
            FROM (SELECT CASE WHEN ({attempts}) % 100 < 85 THEN 1
                              WHEN ({attempts}) % 100 < 96 THEN 2
                              ELSE 3 END AS n_attempts) x
            JOIN generate_series(1, 3) g(attempt_no) ON g.attempt_no <= x.n_attempts
        ) a
        WHERE c.channel_type <> 'marketplace'
          AND c.captured_date >= DATE '{e5}'
    """.format(order_ref=ORDER_REF.format(order_id="c.order_id"),
               attempts=d_attempts,
               outcome=draw(s, "'mp_outcome'", "c.order_id", "a.attempt_no"),
               e5=e5_from))

    # One authorized and one captured event per successful intent, then the
    # populations that only exist outside: refunds, chargebacks, the
    # thirty-day restatement window and the source API's soft delete.
    d_kind = draw(s, "'mp_extra'", "b.captured_date", "b.gateway_reference")
    d_lag = draw(s, "'mp_lag'", "captured_date", "gateway_reference", "n")
    d_fee = draw(s, "'mp_fee'", "gateway_reference")
    d_back = draw(s, "'mp_restate_back'", "gateway_reference")
    d_del = draw(s, "'mp_deleted'", "gateway_reference")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ext.meridian_events AS
        WITH base AS (
            SELECT c.*, i.intent_id, i.order_ref, i.attempt_no
            FROM sim_ext._card_payments c
            JOIN sim_ext.meridian_intents i
              ON i.gateway_reference = c.gateway_reference AND i.outcome = 'succeeded'
            WHERE c.captured_date >= DATE '{e5}'
        ), spread AS (
            SELECT b.*, e.n,
                   CASE e.n WHEN 0 THEN 'authorized' WHEN 1 THEN 'captured'
                            WHEN 2 THEN CASE WHEN ({kind}) % 1000 < 3
                                             THEN 'chargeback' ELSE 'refunded' END
                            ELSE 'captured' END                        AS event_type,
                   e.n = 3                                             AS is_restatement
            FROM base b
            JOIN generate_series(0, 3) e(n)
                 ON e.n <= 1
                 OR (e.n = 2 AND ({kind}) % 1000 < 43)
                 OR (e.n = 3 AND ({kind}) % 1000 >= 500 AND ({kind}) % 1000 < 518)
        )
        SELECT
            'ME-' || lpad((abs(hash(gateway_reference, n)) % 1000000000000)::VARCHAR, 12, '0') AS event_id,
            'PM-' || lpad((payment_id % 10000000)::VARCHAR, 7, '0')    AS payment_id,
            intent_id,
            order_ref,
            'mtx_' || lpad((abs(hash(gateway_reference)) % 100000000)::VARCHAR, 8, '0') AS processor_txn_id,
            gateway_reference,
            event_type,
            CASE event_type WHEN 'refunded' THEN (-amount)::DECIMAL(18,4)
                            WHEN 'chargeback' THEN (-amount)::DECIMAL(18,4)
                            ELSE amount::DECIMAL(18,4) END             AS settled_amount,
            currency_code,
            round(amount * fee_pct, 4)::DECIMAL(18,4)                  AS fee_amount,
            event_time_utc,
            (event_time_utc + INTERVAL 1 DAY * {lag})                  AS loaded_at,
            payout_date                                                AS settlement_date,
            CASE WHEN is_restatement
                 THEN 'ME-' || lpad((abs(hash(gateway_reference, 1)) % 1000000000000)::VARCHAR, 12, '0')
                 END                                                   AS restates_event_id,
            attempt_no,
            CASE WHEN ({deleted}) % 1000 < 3
                 THEN date_trunc('minute', captured_at + INTERVAL 9 DAY) END AS deleted_at
        FROM (
            SELECT spread.*,
                   date_trunc('minute', captured_at
                       + INTERVAL 1 MINUTE * CASE n WHEN 0 THEN -2 ELSE 0 END
                       + INTERVAL 1 DAY * CASE WHEN n = 2 THEN 6 + ({fee}) % 40
                                               WHEN n = 3 THEN {back} ELSE 0 END) AS event_time_utc
            FROM spread
        )
    """.format(e5=e5_from, kind=d_kind, lag=lag_days(d_lag, tail),
               fee=d_fee, deleted=d_del,
               back=uniform(d_back, 28, 31)))


def _marketplace(ctx: Context) -> None:
    """The operator's own settlement platform.

    Seller-facing, not order-facing. The principal is money Copperline owes
    the seller, so it signs negative from the operator's side, and GMV is
    the seller's number and never Copperline revenue.
    """
    s = ctx.seed
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ext.marketplace_sellers AS
        SELECT t.i                                              AS seller_seq,
               'MS-' || lpad((1000 + t.i * 3)::VARCHAR, 5, '0')  AS seller_id,
               'Seller ' || lpad((1000 + t.i * 3)::VARCHAR, 5, '0') AS seller_name,
               ({bps})                                          AS commission_rate_bps,
               'USD'                                            AS payout_currency
        FROM generate_series(0, {last}) t(i)
        ORDER BY t.i
    """.format(bps=uniform(draw(s, "'mkt_bps'", "t.i"), 600, 1800),
               last=MARKETPLACE_SELLERS - 1))

    d_seller = draw(s, "'mkt_seller'", "o.order_id")
    d_ship = draw(s, "'mkt_ship'", "o.order_id")
    d_state = draw(s, "'mkt_state'", "o.order_id")
    d_lag = draw(s, "'mkt_lag'", "o.order_id")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ext.marketplace_orders AS
        SELECT
            'MO-' || lpad((o.order_id % 100000000)::VARCHAR, 8, '0') AS marketplace_order_id,
            o.order_id,
            sl.seller_id,
            o.grand_total::DECIMAL(18,4)                             AS gmv_amount,
            round(o.grand_total * sl.commission_rate_bps / 10000.0, 2)::DECIMAL(18,4) AS commission_amount,
            round(o.grand_total * 0.0180, 2)::DECIMAL(18,4)          AS fulfilment_fee_amount,
            sl.commission_rate_bps,
            o.currency_code,
            date_trunc('minute', o.order_datetime)                   AS placed_at,
            -- The seller's ship confirmation is the recognition date, not the
            -- order date. REV-14 turns on exactly this column.
            (o.order_datetime + INTERVAL 1 DAY * (1 + ({ship}) % 6))::DATE AS ship_confirmed_date,
            CASE ({state}) % 100 WHEN 0 THEN 'cancelled'
                 WHEN 1 THEN 'refunded' WHEN 2 THEN 'refunded'
                 ELSE 'shipped' END                                  AS order_state,
            (date_trunc('minute', o.order_datetime)
                + INTERVAL 1 DAY * {lag})                            AS loaded_at
        FROM sim_sales.orders o
        JOIN sim_store.channels ch ON ch.channel_id = o.channel_id
                                  AND ch.channel_type = 'marketplace'
        JOIN sim_ext.marketplace_sellers sl ON sl.seller_seq = ({seller}) % {n}
    """.format(ship=d_ship, state=d_state, seller=d_seller, n=MARKETPLACE_SELLERS,
               lag=lag_days(d_lag, ctx.cfg["late_tail"]["marketplace"])))

    # Payouts run weekly, on the Friday after the ship confirmation.
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ext.marketplace_settlements AS
        WITH payable AS (
            SELECT m.*,
                   (m.ship_confirmed_date
                        + INTERVAL 1 DAY * ((12 - dayofweek(m.ship_confirmed_date)) % 7 + 3))::DATE
                        AS payout_date
            FROM sim_ext.marketplace_orders m
            WHERE m.order_state <> 'cancelled'
        ), lines AS (
            SELECT marketplace_order_id, order_id, seller_id, currency_code,
                   payout_date, ship_confirmed_date, loaded_at,
                   'principal' AS line_type,
                   (-gmv_amount)::DECIMAL(18,4) AS amount FROM payable
            UNION ALL
            SELECT marketplace_order_id, order_id, seller_id, currency_code,
                   payout_date, ship_confirmed_date, loaded_at,
                   'commission', commission_amount FROM payable
            UNION ALL
            SELECT marketplace_order_id, order_id, seller_id, currency_code,
                   payout_date, ship_confirmed_date, loaded_at,
                   'fulfilment_fee', fulfilment_fee_amount FROM payable
            UNION ALL
            -- A marketplace return reduces commission in the period of the
            -- return and never restates the original period.
            SELECT marketplace_order_id, order_id, seller_id, currency_code,
                   (payout_date + INTERVAL 21 DAY)::DATE, ship_confirmed_date, loaded_at,
                   'refund', (-commission_amount)::DECIMAL(18,4)
            FROM payable WHERE order_state = 'refunded'
        )
        SELECT
            'MSET-' || lpad((abs(hash(marketplace_order_id, line_type)) % 1000000000000)::VARCHAR, 12, '0') AS settlement_id,
            'MPO-' || seller_id || '-' || strftime(payout_date, '%Y%m%d') AS payout_id,
            marketplace_order_id, order_id, seller_id, line_type,
            amount, currency_code,
            ship_confirmed_date::TIMESTAMP AS posted_at,
            payout_date, loaded_at
        FROM lines
    """)

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ext.marketplace_payouts AS
        SELECT payout_id, seller_id, payout_date,
               sum(amount) FILTER (WHERE line_type = 'principal')::DECIMAL(18,4)      AS principal_amount,
               sum(amount) FILTER (WHERE line_type = 'commission')::DECIMAL(18,4)     AS commission_amount,
               sum(amount) FILTER (WHERE line_type = 'fulfilment_fee')::DECIMAL(18,4) AS fee_amount,
               sum(amount) FILTER (WHERE line_type = 'refund')::DECIMAL(18,4)         AS refund_amount,
               (-sum(amount))::DECIMAL(18,4)                                          AS net_paid_amount,
               'paid'                                                                 AS payout_status
        FROM sim_ext.marketplace_settlements
        GROUP BY payout_id, seller_id, payout_date
    """)


def _marketing(ctx: Context) -> None:
    """Beacon Ads, Tessera Social, Solstice Search and Larkspur.

    Each platform reports its own spend and its own attributed conversions,
    in the market's own currency, and restates three to seven days back. The
    spend table is one row per report date, platform and campaign, so it is
    date-by-entity grain and does not scale with the fact volumes.
    """
    s = ctx.seed
    ctx.sql("CREATE OR REPLACE TABLE sim_ext._ad_markets "
            "(idx INT, market_code VARCHAR, currency_code VARCHAR)")
    ctx.con.executemany(
        "INSERT INTO sim_ext._ad_markets VALUES (?,?,?)",
        [(i, m, c) for i, (m, c) in enumerate(AD_MARKETS)])
    ctx.sql("CREATE OR REPLACE TABLE sim_ext._ad_platforms "
            "(idx INT, platform VARCHAR, display_name VARCHAR, reports_spend BOOLEAN)")
    ctx.con.executemany("INSERT INTO sim_ext._ad_platforms VALUES (?,?,?,?)",
                        [(i, p, d, r) for i, (p, d, r) in enumerate(AD_PLATFORMS)])

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ext.ads_campaigns AS
        SELECT p.platform,
               p.reports_spend,
               c.i                                           AS campaign_seq,
               upper(substr(p.platform, 1, 3)) || '-'
                   || lpad((1000 + c.i)::VARCHAR, 4, '0')     AS campaign_id,
               p.display_name || ' ' || mk.market_code || ' Campaign '
                   || lpad((1 + c.i)::VARCHAR, 3, '0')        AS campaign_name,
               mk.market_code,
               mk.currency_code,
               CASE ({chan}) % 3 WHEN 0 THEN 'search'
                    WHEN 1 THEN 'social' ELSE 'display' END   AS channel
        FROM sim_ext._ad_platforms p
        CROSS JOIN generate_series(0, {last}) c(i)
        JOIN sim_ext._ad_markets mk ON mk.idx = ({mkt}) % 5
        ORDER BY p.platform, c.i
    """.format(last=CAMPAIGNS_PER_PLATFORM - 1,
               chan=draw(s, "'ads_channel'", "p.platform", "c.i"),
               mkt=draw(s, "'ads_market'", "p.platform", "c.i")))

    d_imp = draw(s, "'ads_impr'", "d.ds", "c.campaign_id")
    d_ctr = draw(s, "'ads_ctr'", "d.ds", "c.campaign_id")
    d_cpc = draw(s, "'ads_cpc'", "d.ds", "c.campaign_id")
    d_conv = draw(s, "'ads_conv'", "d.ds", "c.campaign_id")
    d_rest = draw(s, "'ads_restate'", "d.ds", "c.campaign_id")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ext.ads_spend_daily AS
        SELECT
            d.ds AS report_date,
            c.platform, c.campaign_id, c.campaign_name, c.market_code, c.channel,
            impressions,
            clicks,
            round(clicks * cpc_cents / 100.0, 2)::DECIMAL(18,4)  AS spend_amount,
            c.currency_code,
            attributed_orders,
            round(attributed_orders * (38 + ({conv}) % 90), 2)::DECIMAL(18,4) AS attributed_revenue,
            (d.ds + INTERVAL 1 DAY)::TIMESTAMP + INTERVAL 4 HOUR AS loaded_at,
            -- Ad platforms restate three to seven days back, which is why a
            -- freshness alarm on this table has an honest reason to look wrong.
            CASE WHEN ({rest}) % 100 < 9
                 THEN (d.ds + INTERVAL 1 DAY * (3 + ({rest}) % 5))::TIMESTAMP END AS restated_at
        FROM (SELECT range::DATE AS ds
              FROM range(DATE '{start}', DATE '{end}' + INTERVAL 1 DAY, INTERVAL 1 DAY)) d
        CROSS JOIN (SELECT * FROM sim_ext.ads_campaigns WHERE reports_spend) c
        CROSS JOIN LATERAL (SELECT ({imp}) AS impressions) i
        CROSS JOIN LATERAL (SELECT (i.impressions * (8 + ({ctr}) % 34) / 1000)::BIGINT AS clicks) k
        CROSS JOIN LATERAL (SELECT ({cpc}) AS cpc_cents) p
        CROSS JOIN LATERAL (SELECT (k.clicks * (10 + ({conv}) % 45) / 1000)::BIGINT AS attributed_orders) a
    """.format(start=ctx.start, end=ctx.end, imp=uniform(d_imp, 900, 240000),
               ctr=d_ctr, cpc=uniform(d_cpc, 22, 480), conv=d_conv, rest=d_rest))

    # Larkspur. Deliberately small: the table exists for the derived-column
    # governance set and one join, and does not need to be big to do that.
    per_day = max(1, ctx.scale(70))
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ext._email_audience AS
        SELECT row_number() OVER (ORDER BY customer_id) - 1 AS i, customer_id
        FROM (SELECT DISTINCT customer_id FROM sim_sales.orders
              WHERE customer_id IS NOT NULL ORDER BY customer_id LIMIT 20000)
    """)
    n_aud = ctx.sql("SELECT count(*) FROM sim_ext._email_audience").fetchone()[0]
    d_evt = draw(ctx.seed, "'email_type'", "d.ds", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ext.email_events AS
        SELECT
            'LS-' || strftime(d.ds, '%Y%m%d') || '-' || lpad(g.i::VARCHAR, 6, '0') AS send_id,
            c.campaign_id,
            au.customer_id                                        AS customer_ref,
            CASE ({evt}) % 100 WHEN 0 THEN 'unsub' WHEN 1 THEN 'bounce'
                 WHEN 2 THEN 'bounce'
                 ELSE CASE WHEN ({evt}) % 100 < 12 THEN 'click'
                           WHEN ({evt}) % 100 < 44 THEN 'open'
                           WHEN ({evt}) % 100 < 92 THEN 'delivered'
                           ELSE 'sent' END END                     AS event_type,
            (d.ds::TIMESTAMP + INTERVAL 1 MINUTE * (({min}) % 1440)) AS event_time_utc,
            'msg_' || lpad((abs(hash(d.ds, g.i)) % 1000000000)::VARCHAR, 9, '0') AS message_id
        FROM (SELECT range::DATE AS ds
              FROM range(DATE '{start}', DATE '{end}' + INTERVAL 1 DAY, INTERVAL 1 DAY)) d
        CROSS JOIN generate_series(1, {per_day}) g(i)
        JOIN sim_ext._email_audience au ON au.i = ({aud}) % {n_aud}
        JOIN sim_ext.ads_campaigns c ON c.platform = 'larkspur'
                                    AND c.campaign_seq = ({camp}) % {n_camp}
    """.format(n_camp=CAMPAIGNS_PER_PLATFORM,start=ctx.start, end=ctx.end, per_day=per_day, evt=d_evt,
               min=draw(ctx.seed, "'email_min'", "d.ds", "g.i"),
               aud=draw(ctx.seed, "'email_aud'", "d.ds", "g.i"), n_aud=max(1, n_aud),
               camp=draw(ctx.seed, "'email_camp'", "d.ds", "g.i")))
    ctx.sql("DROP TABLE sim_ext._email_audience")


def build(ctx: Context) -> None:
    _card_payments(ctx)
    _halcyon(ctx)
    _meridian(ctx)
    _marketplace(ctx)
    _marketing(ctx)
