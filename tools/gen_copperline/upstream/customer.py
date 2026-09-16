"""`sim_customer` — who buys, and how the company markets to them.

ORDER: **… → northwave → customer → … → sales → …**. It runs after
`northwave`, which builds the shared identity table below, and before `sales`,
because `sales.orders.customer_id` points here.

Two populations share one table, and the split is the whole point.

* **4,000 named trade accounts**, one per row of `sim_identity.trade_accounts`
  — contractors, property managers, small builders, who buy on terms and get
  an `ar_invoice`. `customer_id` is `70000 + trade_seq`, so the identity a
  Copperline row shares with a Northwave row stays in one place and both books
  can point at it. `trade_seq` is the marker: a row with one is a trade
  account, a row without one is a shopper. Entity data — 4,000 at every
  profile, because a dedup problem needs its whole population.
* **Consumer shoppers**, `90000 + consumer_seq`, scaled with the fact volumes
  because they are a by-product of orders rather than a population anyone
  reconciles. They never reach `raw.customers`: the CRM extract is trade only,
  and a consumer order carries an anonymous loyalty id instead. Consumer 412
  is Priya Raman, customer **90412**, the sample storyline, pinned to the row
  chapter 02 prints.

⚠ **The loyalty numbers do not tie, on purpose.** `points_balance` is rebuilt
from the transaction ledger, and then the expiry batch is applied to about one
account in seven the way the real one behaves: it writes the balance down and
does not always write the transaction row. Two numbers, both authoritative,
and a policy sentence that says which one to trust.
"""

from __future__ import annotations

import datetime as dt

from ..config import Context
from ..streams import draw, minute_time, pick, uniform
from . import _common as k
from .hr import DEPT_MARKETING

CONSUMER_COUNT = 300_000        # a fact-shaped population; ctx.scale() applies
TRADE_CHURNED, TRADE_DELETED = 25, 15   # of 4,000; the rest are active
LOYALTY_ENROLLED_PCT = 60
LOYALTY_TXNS_PER_DAY = 1_200
EXPIRY_BATCH_PCT = 14           # accounts whose balance the batch wrote alone
CAMPAIGN_RECIPIENTS = 2_000     # per campaign, before ctx.scale()
PROMOTION_ID_LO, PROMOTION_ID_HI = 700, 799   # product.promotions, drawn not read
WEB_OPENED = dt.date(2018, 3, 5)

PRIYA = {
    "customer_id": k.PRIYA_CUSTOMER_ID,
    "first_name": "Priya",
    "last_name": "Raman",
    "email": "priya.raman@example.com",
    "phone": "+1-347-555-0182",
    "birth_date": dt.date(1991, 6, 4),
    "gender": "F",
    "primary_address_id": 7712045,
    "preferred_store_id": 214,
    "acquisition_channel_id": k.CHANNEL_WEB,
    "signup_date": dt.date(2023, 11, 2),
    "loyalty_account_id": k.PRIYA_LOYALTY_ACCOUNT_ID,
    "tier": "gold",
    "points_balance": 3420,
    "lifetime_points": 18760,
    "enrolled_at": dt.datetime(2023, 11, 2, 14, 22),
}


def build(ctx: Context) -> None:
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_customer")
    _customers(ctx)
    _customer_addresses(ctx)
    _loyalty_accounts(ctx)
    _loyalty_transactions(ctx)
    _settle_loyalty_balances(ctx)
    _marketing(ctx)


def _consumers(ctx: Context) -> int:
    return max(ctx.scale(CONSUMER_COUNT), k.PRIYA_SEQ)


def _customers(ctx: Context) -> None:
    seed = ctx.seed
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_customer.customers (
            customer_id BIGINT PRIMARY KEY,
            customer_code TEXT NOT NULL,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT,
            birth_date DATE,
            gender TEXT,
            primary_address_id BIGINT NOT NULL,
            preferred_store_id INT,
            acquisition_channel_id INT NOT NULL,
            signup_date DATE NOT NULL,
            status TEXT NOT NULL,
            marketing_opt_in BOOL NOT NULL,
            trade_seq INT
        )
    """)

    if k.table_exists(ctx, "sim_identity", "trade_accounts"):
        h = draw(seed, "'customers_trade'", "trade_seq")
        signup_span = (ctx.end - dt.date(2009, 3, 14)).days
        ctx.sql(f"""
            INSERT INTO sim_customer.customers
            WITH ranked AS (
                SELECT t.*,
                       row_number() OVER (ORDER BY {h}) AS status_rank
                FROM sim_identity.trade_accounts t
            )
            SELECT {k.TRADE_CUSTOMER_BASE} + trade_seq,
                   'C-' || lpad(({k.TRADE_CUSTOMER_BASE} + trade_seq)::VARCHAR, 6, '0'),
                   split_part(contact_name, ' ', 1),
                   CASE WHEN instr(contact_name, ' ') > 0
                        THEN trim(substr(contact_name, instr(contact_name, ' ') + 1))
                        ELSE trading_name END,
                   contact_email, phone,
                   NULL, NULL,
                   {uniform(h, k.ADDRESS_ID_LO, k.ADDRESS_ID_HI)},
                   {uniform(f"({h}) // 3", k.STORE_ID_LO, k.STORE_ID_HI)},
                   {k.CHANNEL_TRADE},
                   (DATE '2009-03-14'
                    + (({h}) // 7 % {signup_span}) * INTERVAL 1 DAY)::DATE,
                   CASE WHEN status_rank <= {TRADE_CHURNED} THEN 'churned'
                        WHEN status_rank <= {TRADE_CHURNED + TRADE_DELETED} THEN 'deleted'
                        ELSE 'active' END,
                   ({h}) // 11 % 100 < 74,
                   trade_seq
            FROM ranked
        """)

    n = _consumers(ctx)
    h = draw(seed, "'customers_consumer'", "seq")
    name_h = draw(seed, "'customers_consumer_name'", "seq")
    signup_span = (ctx.end - WEB_OPENED).days
    ctx.sql(f"""
        INSERT INTO sim_customer.customers
        WITH drawn AS (
            SELECT g.seq,
                   {k.CONSUMER_CUSTOMER_BASE} + g.seq AS customer_id,
                   {k.first_name(name_h)} AS first_name,
                   {k.last_name(f"({name_h}) // 37")} AS last_name,
                   {uniform(h, k.ADDRESS_ID_LO, k.ADDRESS_ID_HI)} AS address_id,
                   {uniform(f"({h}) // 3", k.STORE_ID_LO, k.STORE_ID_HI)} AS store_id,
                   {pick(f"({h}) // 5", [(str(k.CHANNEL_STORE), 58),
                                        (str(k.CHANNEL_WEB), 31),
                                        (str(k.CHANNEL_MARKETPLACE), 11)])} AS channel_id,
                   (DATE '{WEB_OPENED}'
                    + (({h}) // 7 % {signup_span}) * INTERVAL 1 DAY)::DATE AS signup_date,
                   (DATE '1948-01-01'
                    + (({h}) // 13 % 21000) * INTERVAL 1 DAY)::DATE AS birth_date,
                   ({h}) // 17 % 100 AS opt_draw,
                   ({h}) // 19 % 1000 AS status_draw
            FROM range(1, {n + 1}) g(seq)
        )
        SELECT customer_id,
               'C-' || lpad(customer_id::VARCHAR, 6, '0'),
               CASE WHEN seq = {k.PRIYA_SEQ} THEN '{PRIYA["first_name"]}'
                    ELSE first_name END,
               CASE WHEN seq = {k.PRIYA_SEQ} THEN '{PRIYA["last_name"]}'
                    ELSE last_name END,
               CASE WHEN seq = {k.PRIYA_SEQ} THEN '{PRIYA["email"]}'
                    ELSE {k.slug('first_name')} || '.' || {k.slug('last_name')}
                         || '.' || customer_id::VARCHAR || '@example.com' END,
               CASE WHEN seq = {k.PRIYA_SEQ} THEN '{PRIYA["phone"]}'
                    ELSE '+1-347-555-' || lpad((({h}) % 10000)::VARCHAR, 4, '0') END,
               CASE WHEN seq = {k.PRIYA_SEQ} THEN DATE '{PRIYA["birth_date"]}'
                    ELSE birth_date END,
               CASE WHEN seq = {k.PRIYA_SEQ} THEN '{PRIYA["gender"]}'
                    ELSE {pick(f"({h}) // 23", [("'F'", 49), ("'M'", 47),
                                               ("'X'", 4)])} END,
               CASE WHEN seq = {k.PRIYA_SEQ} THEN {PRIYA["primary_address_id"]}
                    ELSE address_id END,
               CASE WHEN seq = {k.PRIYA_SEQ} THEN {PRIYA["preferred_store_id"]}
                    ELSE store_id END,
               CASE WHEN seq = {k.PRIYA_SEQ} THEN {PRIYA["acquisition_channel_id"]}
                    ELSE channel_id END,
               CASE WHEN seq = {k.PRIYA_SEQ} THEN DATE '{PRIYA["signup_date"]}'
                    ELSE signup_date END,
               CASE WHEN seq = {k.PRIYA_SEQ} THEN 'active'
                    WHEN status_draw < 61 THEN 'churned' ELSE 'active' END,
               CASE WHEN seq = {k.PRIYA_SEQ} THEN TRUE ELSE opt_draw < 58 END,
               NULL
        FROM drawn
    """)


def _customer_addresses(ctx: Context) -> None:
    """A default shipping address for everyone, and a separate billing address
    for the accounts that keep one."""
    h = draw(ctx.seed, "'customer_addresses'", "c.customer_id", "v.n")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_customer.customer_addresses (
            customer_id BIGINT NOT NULL,
            address_id BIGINT NOT NULL,
            usage_type TEXT NOT NULL,
            is_default BOOL NOT NULL,
            PRIMARY KEY (customer_id, address_id, usage_type)
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_customer.customer_addresses
        SELECT c.customer_id,
               CASE v.n WHEN 1 THEN c.primary_address_id
                        ELSE {uniform(h, k.ADDRESS_ID_LO, k.ADDRESS_ID_HI)} END,
               CASE v.n WHEN 1 THEN 'shipping' ELSE 'billing' END,
               v.n = 1
        FROM sim_customer.customers c
        CROSS JOIN range(1, 3) v(n)
        WHERE v.n = 1
           OR c.trade_seq IS NOT NULL
           OR ({h}) % 100 < 34
    """)


def _loyalty_accounts(ctx: Context) -> None:
    """Loyalty is a consumer programme: trade accounts buy on terms instead.

    `loyalty_account_id` is `43606 + consumer_seq`, an offset chosen so that
    the storyline consumer — seq 412, customer 90412 — holds account 44018,
    the account chapter 02 prints. `card_number` is `6011` followed by
    `900000000 + loyalty_account_id`, which reproduces 6011900044018 for that
    same row.
    """
    h = draw(ctx.seed, "'loyalty_accounts'", "customer_id")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_customer.loyalty_accounts (
            loyalty_account_id BIGINT PRIMARY KEY,
            customer_id BIGINT NOT NULL,
            card_number TEXT NOT NULL,
            tier TEXT NOT NULL,
            points_balance INT NOT NULL,
            lifetime_points INT NOT NULL,
            enrolled_at TIMESTAMP NOT NULL,
            status TEXT NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_customer.loyalty_accounts
        WITH enrolled AS (
            SELECT c.customer_id,
                   c.customer_id - {k.CONSUMER_CUSTOMER_BASE} AS seq,
                   c.signup_date, c.status
            FROM sim_customer.customers c
            WHERE c.trade_seq IS NULL
              AND (({h}) % 100 < {LOYALTY_ENROLLED_PCT}
                   OR c.customer_id = {k.PRIYA_CUSTOMER_ID})
        )
        SELECT {k.LOYALTY_ACCOUNT_BASE} + seq,
               customer_id,
               '6011' || lpad((900000000 + {k.LOYALTY_ACCOUNT_BASE} + seq)::VARCHAR,
                              9, '0'),
               CASE WHEN customer_id = {k.PRIYA_CUSTOMER_ID} THEN '{PRIYA["tier"]}'
                    ELSE {pick(f"({h}) // 3", [("'bronze'", 52), ("'silver'", 28),
                                              ("'gold'", 16), ("'platinum'", 4)])} END,
               0, 0,
               CASE WHEN customer_id = {k.PRIYA_CUSTOMER_ID}
                    THEN TIMESTAMP '{PRIYA["enrolled_at"]}'
                    ELSE {minute_time(f"({h}) // 5", "signup_date")} END,
               CASE WHEN status = 'churned' THEN 'lapsed' ELSE 'active' END
        FROM enrolled
    """)


def _loyalty_transactions(ctx: Context) -> None:
    """Every earn, redeem, expiry and adjustment, drawn per day.

    `order_id` links the earn to the order that caused it whenever `sales` has
    already run. Under the decreed ORDER it has not — `customer` comes first,
    because orders point at customers — so the link is NULL and the ledger
    stands on its own. Move this module after `sales` and the links appear
    with no other change.
    """
    seed = ctx.seed
    n = _consumers(ctx)
    per_day = ctx.scale(LOYALTY_TXNS_PER_DAY)
    h = draw(seed, "'loyalty_transactions'", "d.ds", "g.i")
    txn_type = pick(f"({h}) // 3", [("'earn'", 62), ("'redeem'", 18),
                                   ("'expiry'", 12), ("'adjustment'", 8)])

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_customer.loyalty_transactions (
            loyalty_txn_id BIGINT PRIMARY KEY,
            loyalty_account_id BIGINT NOT NULL,
            order_id BIGINT,
            points_delta INT NOT NULL,
            txn_type TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
    """)
    # The account is worked out in `drawn` and only then joined, so the join
    # carries a plain column equality. Computing it inside the ON clause
    # instead leaves the planner an expression that spans the cross join, and
    # it falls back to a nested loop over every account and every day.
    ctx.sql(f"""
        INSERT INTO sim_customer.loyalty_transactions
        WITH drawn AS (
            SELECT (d.ds - DATE '{ctx.start}')::BIGINT * 1000000 + g.i AS loyalty_txn_id,
                   {k.LOYALTY_ACCOUNT_BASE} + 1 + ({h}) // 101 % {n} AS loyalty_account_id,
                   d.ds,
                   {txn_type} AS txn_type,
                   {h} AS h
            FROM {k.day_spine(ctx)}
            CROSS JOIN range(1, {per_day + 1}) g(i)
        ), events AS (
            SELECT drawn.*
            FROM drawn
            JOIN sim_customer.loyalty_accounts la USING (loyalty_account_id)
            WHERE la.enrolled_at::DATE <= drawn.ds
        )
        SELECT loyalty_txn_id, loyalty_account_id, NULL,
               CASE txn_type
                    WHEN 'earn' THEN 10 + (h) // 7 % 391
                    WHEN 'redeem' THEN -(10 + (h) // 7 % 241)
                    WHEN 'expiry' THEN -(10 + (h) // 7 % 141)
                    ELSE (h) // 7 % 101 - 50 END,
               txn_type,
               {minute_time("h", "ds")}
        FROM events
    """)

    # The enrolment bonus, one row per account on the day it opened. It is
    # what keeps a balance positive: a programme that let a member redeem
    # points they never earned would be a bug in the programme, not a
    # reconciliation the world can ask about.
    bonus = draw(seed, "'loyalty_enrolment_bonus'", "loyalty_account_id")
    ctx.sql(f"""
        INSERT INTO sim_customer.loyalty_transactions
        SELECT 1000000000000 + loyalty_account_id, loyalty_account_id, NULL,
               2000 + ({bonus}) % 6001, 'enrolment_bonus', enrolled_at
        FROM sim_customer.loyalty_accounts
    """)

    if k.table_exists(ctx, "sim_sales", "orders"):
        ctx.sql("""
            UPDATE sim_customer.loyalty_transactions t
            SET order_id = m.order_id
            FROM (
                SELECT t.loyalty_txn_id, min(o.order_id) AS order_id
                FROM sim_customer.loyalty_transactions t
                JOIN sim_customer.loyalty_accounts la USING (loyalty_account_id)
                JOIN sim_sales.orders o
                  ON o.customer_id = la.customer_id
                 AND o.order_datetime::DATE = t.created_at::DATE
                WHERE t.txn_type IN ('earn', 'redeem')
                GROUP BY 1
            ) m
            WHERE m.loyalty_txn_id = t.loyalty_txn_id
        """)


def _settle_loyalty_balances(ctx: Context) -> None:
    """Rebuild the balance from the ledger, then let the expiry batch break it.

    The batch runs on about one account in seven and writes the balance
    without writing the matching transaction row, which is the reconciliation
    the world asks about: two numbers that both look authoritative. The
    storyline account keeps the balance chapter 02 prints, and is one of them.
    """
    h = draw(ctx.seed, "'loyalty_expiry_batch'", "la.loyalty_account_id")
    ctx.sql(f"""
        UPDATE sim_customer.loyalty_accounts la
        SET points_balance = greatest(0, coalesce(t.balance, 0)
                                         - CASE WHEN ({h}) % 100 < {EXPIRY_BATCH_PCT}
                                                THEN 50 + ({h}) // 7 % 951 ELSE 0 END),
            lifetime_points = greatest(coalesce(t.earned, 0), coalesce(t.balance, 0))
        FROM (SELECT loyalty_account_id,
                     sum(points_delta) AS balance,
                     sum(CASE WHEN points_delta > 0 THEN points_delta ELSE 0 END) AS earned
              FROM sim_customer.loyalty_transactions GROUP BY 1) t
        WHERE t.loyalty_account_id = la.loyalty_account_id
    """)
    ctx.sql(f"""
        UPDATE sim_customer.loyalty_accounts
        SET points_balance = {PRIYA["points_balance"]},
            lifetime_points = {PRIYA["lifetime_points"]}
        WHERE loyalty_account_id = {k.PRIYA_LOYALTY_ACCOUNT_ID}
    """)


def _marketing(ctx: Context) -> None:
    """Two campaigns a month, and the funnel each one produced.

    `budget_amount` is what Copperline meant to spend. What the ad platforms
    say was spent is a separate feed, and the two disagree — that difference
    is authored outside this module.
    """
    seed = ctx.seed
    h = draw(seed, "'marketing_campaigns'", "n")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_customer.marketing_campaigns (
            campaign_id INT PRIMARY KEY,
            name TEXT NOT NULL,
            medium TEXT NOT NULL,
            promotion_id INT,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            budget_amount DECIMAL(18,2) NOT NULL,
            cost_center_id INT NOT NULL,
            owner_employee_id INT NOT NULL
        )
    """)
    months = (ctx.end.year - ctx.start.year) * 12 + ctx.end.month - ctx.start.month + 1
    ctx.sql(f"""
        INSERT INTO sim_customer.marketing_campaigns
        WITH slots AS (
            SELECT g.n,
                   (DATE '{ctx.start.replace(day=1)}'
                    + ((g.n - 1) // 2) * INTERVAL 1 MONTH
                    + (((g.n - 1) % 2) * 14) * INTERVAL 1 DAY)::DATE AS start_date
            FROM range(1, {2 * months + 1}) g(n)
        )
        SELECT 1100 + n,
               {pick(h, [("'Spring Basics'", 1), ("'Trade Days'", 1),
                         ("'Outdoor Living'", 1), ("'Weekend Project'", 1),
                         ("'Loyalty Bonus Points'", 1), ("'Clearance Event'", 1)])}
                 || ' ' ||
               {pick(f"({h}) // 3", [("'Email Blast'", 44), ("'Push'", 18),
                                    ("'Paid Social'", 24), ("'Direct Mail'", 14)])},
               {pick(f"({h}) // 3", [("'email'", 44), ("'push'", 18),
                                    ("'paid_social'", 24), ("'direct_mail'", 14)])},
               {uniform(f"({h}) // 5", PROMOTION_ID_LO, PROMOTION_ID_HI)},
               start_date,
               (start_date + INTERVAL 13 DAY)::DATE,
               (2000 + ({h}) // 7 % 38001)::DECIMAL(18,2),
               {k.CC_DEPARTMENT_BASE + DEPT_MARKETING},
               {uniform(f"({h}) // 11", k.EMPLOYEE_ID_LO, k.EMPLOYEE_ID_HI)}
        FROM slots
        WHERE start_date BETWEEN DATE '{ctx.start}' AND DATE '{ctx.end}'
    """)

    n = _consumers(ctx)
    per_campaign = ctx.scale(CAMPAIGN_RECIPIENTS)
    r = draw(seed, "'campaign_responses'", "c.campaign_id", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_customer.campaign_responses (
            response_id BIGINT PRIMARY KEY,
            campaign_id INT NOT NULL,
            customer_id BIGINT NOT NULL,
            sent_at TIMESTAMP NOT NULL,
            opened_at TIMESTAMP,
            clicked_at TIMESTAMP,
            converted_order_id BIGINT,
            unsubscribed BOOL NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_customer.campaign_responses
        WITH sent AS (
            SELECT c.campaign_id::BIGINT * 1000000 + g.i AS response_id,
                   c.campaign_id, c.start_date,
                   {k.CONSUMER_CUSTOMER_BASE} + 1 + ({r}) // 3 % {n} AS customer_id,
                   ({r}) % 1000 AS funnel,
                   {r} AS h
            FROM sim_customer.marketing_campaigns c
            CROSS JOIN range(1, {per_campaign + 1}) g(i)
        )
        SELECT response_id, campaign_id, customer_id,
               start_date::TIMESTAMP + INTERVAL 9 HOUR,
               CASE WHEN funnel < 412
                    THEN start_date::TIMESTAMP + INTERVAL 9 HOUR
                         + ((h) // 7 % 2880) * INTERVAL 1 MINUTE END,
               CASE WHEN funnel < 96
                    THEN start_date::TIMESTAMP + INTERVAL 9 HOUR
                         + ((h) // 11 % 5760) * INTERVAL 1 MINUTE END,
               NULL,
               funnel >= 988
        FROM sent
    """)

    if k.table_exists(ctx, "sim_sales", "orders"):
        ctx.sql("""
            UPDATE sim_customer.campaign_responses cr
            SET converted_order_id = m.order_id
            FROM (
                SELECT cr.response_id, min(o.order_id) AS order_id
                FROM sim_customer.campaign_responses cr
                JOIN sim_sales.orders o
                  ON o.customer_id = cr.customer_id
                 AND o.order_datetime >= cr.clicked_at
                 AND o.order_datetime < cr.clicked_at + INTERVAL 3 DAY
                WHERE cr.clicked_at IS NOT NULL
                GROUP BY 1
            ) m
            WHERE m.response_id = cr.response_id
        """)
