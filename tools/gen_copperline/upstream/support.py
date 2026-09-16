"""`sim_support` — Halyard's service desk, in three tables.

ORDER: **… → sales → ecommerce → support → finance**. It runs after `sales`
so a ticket can name the order it is about.

⚠ **Service levels are inherited.** A root category carries the promised
response and resolution hours; most of its children carry NULL and a few
override. Measuring attainment therefore means walking the tree until a level
is set, which is the point of the table.
"""

from __future__ import annotations

from ..config import Context
from ..streams import draw, minute_time, pick, uniform
from . import _common as k
from .hr import DEPT_CONTACT_CENTRE

TICKETS_PER_DAY = 104           # ~90,000 over the range, before ctx.scale()
DEPT_RETURNS_AND_CLAIMS = 32

# (category_id, name, parent, sla_response_hours, sla_resolution_hours, dept).
# A NULL pair inherits from the parent. Category 52 is the spec's sample and
# carries its own 4/48, which is also what its parent promises.
CATEGORIES = [
    (40, "Delivery", None, 4, 48, DEPT_CONTACT_CENTRE),
    (41, "Order Issues", None, 4, 24, DEPT_CONTACT_CENTRE),
    (42, "Product", None, 8, 72, DEPT_CONTACT_CENTRE),
    (43, "Returns and Refunds", None, 4, 48, DEPT_RETURNS_AND_CLAIMS),
    (44, "Account and Loyalty", None, 12, 96, DEPT_CONTACT_CENTRE),
    (45, "Trade Account", None, 2, 24, DEPT_CONTACT_CENTRE),
    (46, "Payment", None, 4, 24, DEPT_CONTACT_CENTRE),
    (50, "Delivery - Damaged in Transit", 40, None, None, DEPT_CONTACT_CENTRE),
    (51, "Delivery - Missing Parcel", 40, 2, None, DEPT_CONTACT_CENTRE),
    (52, "Delivery - Late Arrival", 40, 4, 48, DEPT_CONTACT_CENTRE),
    (53, "Delivery - Wrong Address", 40, None, None, DEPT_CONTACT_CENTRE),
    (54, "Order - Cancelled in Error", 41, None, None, DEPT_CONTACT_CENTRE),
    (55, "Order - Wrong Item Shipped", 41, None, 48, DEPT_CONTACT_CENTRE),
    (56, "Order - Partial Shipment", 41, None, None, DEPT_CONTACT_CENTRE),
    (57, "Product - Damaged on Arrival", 42, 4, None, DEPT_CONTACT_CENTRE),
    (58, "Product - Missing Parts", 42, None, None, DEPT_CONTACT_CENTRE),
    (59, "Product - Assembly Question", 42, None, None, DEPT_CONTACT_CENTRE),
    (60, "Returns - Refund Not Received", 43, None, 24, DEPT_RETURNS_AND_CLAIMS),
    (61, "Returns - Label Request", 43, None, None, DEPT_RETURNS_AND_CLAIMS),
    (62, "Returns - Restocking Fee Query", 43, None, None, DEPT_RETURNS_AND_CLAIMS),
    (63, "Account - Login Problem", 44, None, None, DEPT_CONTACT_CENTRE),
    (64, "Account - Loyalty Points Missing", 44, None, 48, DEPT_CONTACT_CENTRE),
    (65, "Account - Marketing Preferences", 44, None, None, DEPT_CONTACT_CENTRE),
    (66, "Trade - Credit Limit", 45, None, None, DEPT_CONTACT_CENTRE),
    (67, "Trade - Invoice Query", 45, None, None, DEPT_CONTACT_CENTRE),
    (68, "Payment - Declined Card", 46, None, None, DEPT_CONTACT_CENTRE),
    (69, "Payment - Duplicate Charge", 46, 1, 8, DEPT_CONTACT_CENTRE),
]

_LEAF_IDS = [c[0] for c in CATEGORIES if c[2] is not None]

SUBJECTS = [
    "Parcel has not arrived", "Wrong size, need exchange", "Missing fixings pack",
    "Refund not showing on my card", "Cannot sign in to my account",
    "Points missing from last order", "Charged twice for one order",
    "Delivery left at the wrong door", "Need a copy of the invoice",
    "Credit limit query on the trade account",
]


def build(ctx: Context) -> None:
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_support")
    _categories(ctx)
    _tickets(ctx)
    _messages(ctx)


def _categories(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_support.ticket_categories (
            category_id INT PRIMARY KEY,
            name TEXT NOT NULL,
            parent_category_id INT,
            sla_response_hours INT,
            sla_resolution_hours INT,
            owning_department_id INT NOT NULL
        )
    """)
    ctx.con.executemany(
        "INSERT INTO sim_support.ticket_categories VALUES (?,?,?,?,?,?)", CATEGORIES
    )


def _tickets(ctx: Context) -> None:
    seed = ctx.seed
    per_day = ctx.scale(TICKETS_PER_DAY)
    h = draw(seed, "'tickets'", "d.ds", "g.i")
    trade_pick = f"({h}) // 3 % 100 < 12"
    consumers = ctx.con.execute(
        "SELECT count(*) FROM sim_customer.customers WHERE trade_seq IS NULL"
    ).fetchone()[0] if k.table_exists(ctx, "sim_customer", "customers") else 0
    trades = ctx.con.execute(
        "SELECT count(*) FROM sim_customer.customers WHERE trade_seq IS NOT NULL"
    ).fetchone()[0] if consumers else 0

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_support.tickets (
            ticket_id BIGINT PRIMARY KEY,
            ticket_number TEXT NOT NULL,
            customer_id BIGINT,
            order_id BIGINT,
            return_id BIGINT,
            shipment_id BIGINT,
            category_id INT NOT NULL,
            contact_channel TEXT NOT NULL,
            subject TEXT NOT NULL,
            priority TEXT NOT NULL,
            ticket_status TEXT NOT NULL,
            opened_at TIMESTAMP NOT NULL,
            first_response_at TIMESTAMP,
            closed_at TIMESTAMP,
            assigned_employee_id INT,
            csat_score INT
        )
    """)
    if not consumers:
        return

    customer_expr = (
        f"CASE WHEN {trade_pick} AND {trades} > 0 "
        f"THEN {k.TRADE_CUSTOMER_BASE} + 1 + ({h}) // 5 % {max(trades, 1)} "
        f"ELSE {k.CONSUMER_CUSTOMER_BASE} + 1 + ({h}) // 5 % {consumers} END"
    )
    ctx.sql(f"""
        INSERT INTO sim_support.tickets
        WITH opened AS (
            SELECT 6600000 + (d.ds - DATE '{ctx.start}')::BIGINT * 1000 + g.i AS ticket_id,
                   {customer_expr} AS customer_id,
                   {pick(f"({h}) // 7", [(str(c), 1) for c in _LEAF_IDS])} AS category_id,
                   {minute_time(h, "d.ds")} AS opened_at,
                   ({h}) % 1000 AS lifecycle,
                   {h} AS h
            FROM {k.day_spine(ctx)}
            CROSS JOIN range(1, {per_day + 1}) g(i)
        )
        SELECT ticket_id, 'TK-' || ticket_id::VARCHAR, customer_id,
               NULL, NULL, NULL, category_id,
               {pick("(h) // 11", [("'chat'", 34), ("'email'", 31),
                                  ("'phone'", 27), ("'social'", 8)])},
               {k.sql_list(SUBJECTS)}[1 + (h) // 13 % {len(SUBJECTS)}],
               {pick("(h) // 17", [("'low'", 22), ("'normal'", 58),
                                  ("'high'", 16), ("'urgent'", 4)])},
               CASE WHEN lifecycle < 902 THEN 'resolved'
                    WHEN lifecycle < 951 THEN 'closed'
                    WHEN lifecycle < 981 THEN 'pending' ELSE 'open' END,
               opened_at,
               CASE WHEN lifecycle < 981
                    THEN opened_at + (7 + (h) // 19 % 600) * INTERVAL 1 MINUTE END,
               CASE WHEN lifecycle < 951
                    THEN opened_at + (90 + (h) // 23 % 5400) * INTERVAL 1 MINUTE END,
               {uniform("(h) // 29", k.EMPLOYEE_ID_LO, k.EMPLOYEE_ID_HI)},
               CASE WHEN lifecycle < 951 THEN 1 + (h) // 31 % 5 END
        FROM opened
    """)

    if k.table_exists(ctx, "sim_sales", "orders"):
        # The ticket is about the customer's most recent order in the fortnight
        # before they got in touch. Tickets with no such order keep a NULL.
        ctx.sql("""
            UPDATE sim_support.tickets t
            SET order_id = m.order_id
            FROM (
                SELECT t.ticket_id, max(o.order_id) AS order_id
                FROM sim_support.tickets t
                JOIN sim_sales.orders o
                  ON o.customer_id = t.customer_id
                 AND o.order_datetime <= t.opened_at
                 AND o.order_datetime > t.opened_at - INTERVAL 14 DAY
                GROUP BY 1
            ) m
            WHERE m.ticket_id = t.ticket_id
        """)


def _messages(ctx: Context) -> None:
    """The back-and-forth, internal notes included."""
    h = draw(ctx.seed, "'ticket_messages'", "t.ticket_id", "v.n")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_support.ticket_messages (
            message_id BIGINT PRIMARY KEY,
            ticket_id BIGINT NOT NULL,
            sender_type TEXT NOT NULL,
            sender_employee_id INT,
            sender_customer_id BIGINT,
            body TEXT NOT NULL,
            sent_at TIMESTAMP NOT NULL,
            is_internal_note BOOL NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_support.ticket_messages
        WITH thread AS (
            SELECT t.ticket_id, t.customer_id, t.assigned_employee_id,
                   t.opened_at, t.subject, v.n, {h} AS h
            FROM sim_support.tickets t
            CROSS JOIN range(1, 7) v(n)
            WHERE v.n <= 2 + ({h}) % 5
        )
        SELECT ticket_id * 10 + n, ticket_id,
               CASE WHEN n % 2 = 1 THEN 'customer'
                    WHEN (h) % 100 < 12 THEN 'internal' ELSE 'agent' END,
               CASE WHEN n % 2 = 0 THEN assigned_employee_id END,
               CASE WHEN n % 2 = 1 THEN customer_id END,
               CASE WHEN n = 1 THEN subject
                    WHEN n % 2 = 1 THEN 'Following up on this, please.'
                    WHEN (h) % 100 < 12 THEN 'Checked the carrier scan, no delivery event.'
                    ELSE 'Thanks for getting in touch - looking into it now.' END,
               opened_at + (n * (11 + (h) % 240)) * INTERVAL 1 MINUTE,
               n % 2 = 0 AND (h) % 100 < 12
        FROM thread
    """)
