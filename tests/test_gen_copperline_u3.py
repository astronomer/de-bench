"""The customer, finance, support and HR halves of the copperline upstream.

These modules sit either side of `sales` in the build order, and `sales` is
written by another author. So the tests here build the four modules against a
stub of the sales tables — chapter 02's DDL, a few hundred rows — rather than
against the whole world. That keeps the run quick and it keeps the assertions
about this code rather than about someone else's volumes.

What is asserted is what the spec pins and what a task will grade against:
debits equal credits on every entry in the ledger, only trade orders carry a
receivable, the trade book is 4,000 accounts, the loyalty balance and the
loyalty ledger disagree on purpose, customer 90412 is the row chapter 02
prints, and two builds of the same config are the same world.
"""

import datetime as dt
import sys
from pathlib import Path

import duckdb
import pytest

from de_bench.tasks import repo_root

sys.path.insert(0, str(repo_root() / "tools"))

from gen_copperline import config                                # noqa: E402
from gen_copperline.upstream import (                            # noqa: E402
    _common as k, customer, finance, finance_reference, hr, support,
)

WORLD = repo_root() / "worlds" / "copperline"
TRADE_ACCOUNTS = 4_000
STUB_ORDERS = 600


def _context(tmp_path: Path, name: str = "u3.duckdb") -> config.Context:
    cfg = config.load_timeline(WORLD / "timeline.yaml")
    tmp_path.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(tmp_path / name))
    return config.Context(con=con, cfg=cfg, profile="small", landing=tmp_path)


def _stub_identity(ctx: config.Context, rows: int = TRADE_ACCOUNTS) -> None:
    """`sim_identity.trade_accounts` as the northwave module builds it: one row
    per named trade account, shared by the Copperline and Northwave books."""
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_identity")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_identity.trade_accounts AS
        SELECT g.trade_seq::INT AS trade_seq,
               'Trade Account ' || g.trade_seq::VARCHAR || ' LLC' AS legal_name,
               'Trade Account ' || g.trade_seq::VARCHAR AS trading_name,
               'Dana Whitfield' AS contact_name,
               'contact' || g.trade_seq::VARCHAR || '@example.com' AS contact_email,
               '+1-503-555-' || lpad((g.trade_seq % 10000)::VARCHAR, 4, '0') AS phone,
               g.trade_seq::VARCHAR || ' Mill Street' AS address_line1,
               'Portland' AS city,
               'OR' AS region_code,
               '97201' AS postal_code,
               'US' AS country_code,
               'TAX' || lpad(g.trade_seq::VARCHAR, 7, '0') AS tax_id
        FROM range(1, {rows + 1}) g(trade_seq)
    """)


def _stub_sales(ctx: config.Context, orders: int = STUB_ORDERS) -> None:
    """`sim_sales.orders`, `order_lines` and `returns` at chapter 02's column
    list. Channel 4 is the trade book, and it is every third order here so the
    receivable population is big enough to count."""
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_sales")
    days = (ctx.end - ctx.start).days
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_sales.orders AS
        WITH drawn AS (
            SELECT 8800000 + g.i AS order_id,
                   (DATE '{ctx.start}' + (g.i * 3 % {days}) * INTERVAL 1 DAY)::DATE AS ds,
                   1 + g.i % 4 AS channel_id,
                   round(20 + g.i % 400 + g.i % 7 / 4.0, 4)::DECIMAL(18,4) AS subtotal,
                   g.i
            FROM range(1, {orders + 1}) g(i)
        )
        SELECT order_id,
               'W-' || order_id::VARCHAR AS order_number,
               {k.CONSUMER_CUSTOMER_BASE} + 1 + i % 900 AS customer_id,
               channel_id,
               CASE WHEN channel_id = 1 THEN {k.STORE_ID_LO} + i % 40 END AS store_id,
               CASE WHEN channel_id = 1 THEN ({k.STORE_ID_LO} + i % 40) * 10 END
                 AS register_id,
               CASE WHEN channel_id = 1 THEN {k.EMPLOYEE_ID_LO} + i % 500 END
                 AS cashier_employee_id,
               11 AS price_list_id,
               ds::TIMESTAMP + INTERVAL 19 HOUR AS order_datetime,
               'shipped' AS order_status,
               CASE i % 5 WHEN 0 THEN 'GBP' WHEN 1 THEN 'EUR' ELSE 'USD' END
                 AS currency_code,
               subtotal AS subtotal_amount,
               round(subtotal * 0.1, 4)::DECIMAL(18,4) AS order_discount_amount,
               round(subtotal * 0.07, 4)::DECIMAL(18,4) AS tax_amount,
               4.9900::DECIMAL(18,4) AS shipping_amount,
               (subtotal - round(subtotal * 0.1, 4) + round(subtotal * 0.07, 4)
                + 4.99)::DECIMAL(18,4) AS grand_total,
               7712045::BIGINT AS billing_address_id,
               7712045::BIGINT AS shipping_address_id
        FROM drawn
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales.order_lines AS
        SELECT o.order_id * 10 + v.n AS order_line_id, o.order_id, v.n AS line_no,
               550231::BIGINT AS variant_id,
               1.000::DECIMAL(12,3) AS qty,
               round(o.subtotal_amount / 2, 4)::DECIMAL(18,4) AS unit_price,
               round(o.subtotal_amount / 5, 4)::DECIMAL(18,4) AS unit_cost,
               0.0000::DECIMAL(18,4) AS line_discount_amount,
               204 AS tax_rate_id,
               round(o.tax_amount / 2, 4)::DECIMAL(18,4) AS tax_amount,
               round(o.subtotal_amount / 2, 4)::DECIMAL(18,4) AS line_total,
               NULL::BIGINT AS fulfillment_id,
               'shipped' AS line_status
        FROM sim_sales.orders o CROSS JOIN range(1, 3) v(n)
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales.returns AS
        SELECT 440000 + row_number() OVER (ORDER BY o.order_id) AS return_id,
               'RMA-' || o.order_id::VARCHAR AS rma_number,
               o.order_id, o.customer_id, o.store_id,
               o.order_datetime + INTERVAL 9 DAY AS requested_at,
               NULL::INT AS approved_by_employee_id,
               'wrong_size' AS reason_code,
               'completed' AS return_status,
               round(o.grand_total / 4, 4)::DECIMAL(18,4) AS refund_amount,
               NULL::BIGINT AS refund_payment_id
        FROM sim_sales.orders o WHERE o.order_id % 11 = 0
    """)


def _build_world(tmp_path: Path, name: str = "u3.duckdb") -> config.Context:
    """The four modules in their decreed ORDER position, with the sales module
    stubbed in the slot it occupies between `customer` and `support`."""
    ctx = _context(tmp_path, name)
    hr.build(ctx)
    finance_reference.build(ctx)
    _stub_identity(ctx)
    customer.build(ctx)
    _stub_sales(ctx)
    support.build(ctx)
    finance.build(ctx)
    return ctx


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    ctx = _build_world(tmp_path_factory.mktemp("u3"))
    yield ctx
    ctx.con.close()


def test_every_journal_entry_balances(world):
    """The ledger's built-in integrity check, over the whole general ledger and
    in both currencies: debits equal credits on every single entry."""
    entries, lines = world.con.execute(
        "SELECT (SELECT count(*) FROM sim_finance.journal_entries), count(*) "
        "FROM sim_finance.journal_lines"
    ).fetchone()
    assert entries > 500 and lines > 2 * entries

    unbalanced = world.con.execute("""
        SELECT journal_entry_id, sum(debit_amount), sum(credit_amount)
        FROM sim_finance.journal_lines
        GROUP BY 1
        HAVING sum(debit_amount) <> sum(credit_amount)
        LIMIT 5
    """).fetchall()
    assert unbalanced == []

    base_unbalanced = world.con.execute("""
        SELECT journal_entry_id
        FROM sim_finance.journal_lines
        GROUP BY 1
        HAVING sum(CASE WHEN debit_amount > 0 THEN base_amount ELSE 0 END)
            <> sum(CASE WHEN credit_amount > 0 THEN base_amount ELSE 0 END)
        LIMIT 5
    """).fetchall()
    assert base_unbalanced == []

    # Every entry has lines, and every line has an entry.
    orphans = world.con.execute("""
        SELECT count(*) FROM sim_finance.journal_entries e
        WHERE NOT EXISTS (SELECT 1 FROM sim_finance.journal_lines l
                          WHERE l.journal_entry_id = e.journal_entry_id)
    """).fetchone()[0]
    assert orphans == 0


def test_a_currency_entry_balances_after_conversion(world):
    """The euro and sterling entries are the ones that could round apart."""
    n = world.con.execute("""
        SELECT count(DISTINCT journal_entry_id) FROM sim_finance.journal_lines
        WHERE currency_code IN ('GBP', 'EUR') AND fx_rate <> 1
    """).fetchone()[0]
    assert n > 50


def test_only_trade_orders_carry_a_receivable(world):
    """⚠ Store and web orders settle at the till or the gateway. About two
    thirds of revenue has no `ar_invoice` row at all, and a receivables number
    that reads this table as the revenue population is wrong by construction."""
    covered = world.con.execute("""
        SELECT o.channel_id, count(*), count(i.ar_invoice_id)
        FROM sim_sales.orders o
        LEFT JOIN sim_finance.ar_invoices i USING (order_id)
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    for channel_id, orders, invoiced in covered:
        assert orders > 0
        assert invoiced == (orders if channel_id == k.CHANNEL_TRADE else 0), channel_id

    trade_revenue, all_revenue = world.con.execute(f"""
        SELECT sum(CASE WHEN channel_id = {k.CHANNEL_TRADE} THEN grand_total ELSE 0 END),
               sum(grand_total)
        FROM sim_sales.orders
    """).fetchone()
    assert 0 < trade_revenue < all_revenue

    # The invoice points at the entry whose debit created the receivable.
    mismatched = world.con.execute("""
        SELECT count(*) FROM sim_finance.ar_invoices i
        JOIN sim_finance.journal_lines l
          ON l.journal_entry_id = i.journal_entry_id AND l.account_id = 1200
        WHERE l.debit_amount <> i.total_amount
    """).fetchone()[0]
    assert mismatched == 0

    assert world.con.execute(
        "SELECT count(*) FROM sim_finance.ar_invoices WHERE amount_settled > total_amount"
    ).fetchone()[0] == 0


def test_the_trade_book_is_four_thousand_accounts(world):
    """Entity data: 4,000 named trade accounts at every profile, because a
    dedup problem needs its whole population. Consumers scale, trade does not."""
    trade, consumer = world.con.execute("""
        SELECT count(*) FILTER (WHERE trade_seq IS NOT NULL),
               count(*) FILTER (WHERE trade_seq IS NULL)
        FROM sim_customer.customers
    """).fetchone()
    assert trade == TRADE_ACCOUNTS
    assert consumer >= k.PRIYA_SEQ

    # 3,960 of them active — the count the CRM extract and the board pack argue over.
    assert world.con.execute(
        "SELECT count(*) FROM sim_customer.customers "
        "WHERE trade_seq IS NOT NULL AND status = 'active'"
    ).fetchone()[0] == 3_960

    # Trade ids are 70000 + trade_seq, and no consumer sits in that band.
    assert world.con.execute(f"""
        SELECT count(*) FROM sim_customer.customers
        WHERE trade_seq IS NOT NULL
          AND customer_id <> {k.TRADE_CUSTOMER_BASE} + trade_seq
    """).fetchone()[0] == 0

    # Loyalty is a consumer programme; trade accounts buy on terms instead.
    assert world.con.execute("""
        SELECT count(*) FROM sim_customer.loyalty_accounts la
        JOIN sim_customer.customers c USING (customer_id)
        WHERE c.trade_seq IS NOT NULL
    """).fetchone()[0] == 0


def test_the_loyalty_ledger_does_not_tie_to_the_balance(world):
    """⚠ Summed deltas do not tie to `points_balance` on every account. The
    expiry batch writes the balance and sometimes not the delta, which is the
    shape a reconciliation task needs: two numbers that both look
    authoritative and one rule that says which one to trust."""
    total, off = world.con.execute("""
        WITH ledger AS (
            SELECT la.loyalty_account_id, la.points_balance,
                   coalesce(sum(t.points_delta), 0) AS summed
            FROM sim_customer.loyalty_accounts la
            LEFT JOIN sim_customer.loyalty_transactions t USING (loyalty_account_id)
            GROUP BY 1, 2
        )
        SELECT count(*), count(*) FILTER (WHERE points_balance <> summed)
        FROM ledger WHERE summed <> 0
    """).fetchone()
    assert total > 100, "no account has a ledger to disagree with"
    assert 0 < off < total, (total, off)

    # It is a minority, so a profile of the two columns does not shout.
    assert off / total < 0.5


def test_customer_90412_is_priya_raman(world):
    """The sample storyline, pinned: every join chapter 02 prints resolves."""
    row = world.con.execute(f"""
        SELECT customer_code, first_name, last_name, email, phone, birth_date,
               gender, primary_address_id, preferred_store_id,
               acquisition_channel_id, signup_date, status, marketing_opt_in,
               trade_seq
        FROM sim_customer.customers WHERE customer_id = {k.PRIYA_CUSTOMER_ID}
    """).fetchone()
    assert row == ("C-090412", "Priya", "Raman", "priya.raman@example.com",
                   "+1-347-555-0182", dt.date(1991, 6, 4), "F", 7712045, 214,
                   k.CHANNEL_WEB, dt.date(2023, 11, 2), "active", True, None)

    assert world.con.execute(f"""
        SELECT loyalty_account_id, card_number, tier, points_balance,
               lifetime_points, enrolled_at, status
        FROM sim_customer.loyalty_accounts
        WHERE customer_id = {k.PRIYA_CUSTOMER_ID}
    """).fetchone() == (44018, "6011900044018", "gold", 3420, 18760,
                        dt.datetime(2023, 11, 2, 14, 22), "active")


def test_the_fiscal_calendar_closes_the_months_the_spec_names(world):
    """28 closed months as of world today, FY2024 P1 through FY2026 P4, and
    the June period still open."""
    closed = world.con.execute("""
        SELECT count(*) FROM sim_finance.fiscal_periods
        WHERE period_status = 'closed' AND fiscal_period_id >= 202401
    """).fetchone()[0]
    assert closed == 28

    assert world.con.execute(
        "SELECT period_status, period_start, period_end FROM sim_finance.fiscal_periods "
        "WHERE fiscal_period_id = 202604"
    ).fetchone() == ("closed", dt.date(2026, 5, 3), dt.date(2026, 5, 30))
    assert world.con.execute(
        "SELECT period_status FROM sim_finance.fiscal_periods "
        "WHERE fiscal_period_id = 202605"
    ).fetchone() == ("open",)

    # The spec's sample period, FY2026 P3.
    assert world.con.execute(
        "SELECT period_start, period_end, period_status FROM sim_finance.fiscal_periods "
        "WHERE fiscal_period_id = 202603"
    ).fetchone() == (dt.date(2026, 4, 5), dt.date(2026, 5, 2), "closed")

    # Every entry lands in a period; none falls through the calendar.
    assert world.con.execute(
        "SELECT count(*) FROM sim_finance.journal_entries WHERE fiscal_period_id = 0"
    ).fetchone()[0] == 0


def test_five_legal_entities_and_none_books_before_it_traded(world):
    entities = world.con.execute(
        "SELECT entity_code FROM sim_finance.legal_entities ORDER BY legal_entity_id"
    ).fetchall()
    assert [e[0] for e in entities] == ["CL-US", "CL-GB", "CL-IE", "CL-DE", "CL-MX"]

    early = world.con.execute("""
        SELECT count(*) FROM sim_finance.journal_entries e
        JOIN sim_finance.legal_entities le USING (legal_entity_id)
        WHERE e.entry_date < le.trading_from
    """).fetchone()[0]
    assert early == 0


def test_the_circular_department_head_resolves(world):
    """`employees.department_id` and `departments.head_employee_id` point at
    each other. The load order is departments, then employees, then an UPDATE
    that sets the heads — so every head must exist and still work here."""
    dangling = world.con.execute("""
        SELECT count(*) FROM sim_hr.departments d
        WHERE d.head_employee_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM sim_hr.employees e
                          WHERE e.employee_id = d.head_employee_id
                            AND e.department_id = d.department_id
                            AND e.termination_date IS NULL)
    """).fetchone()[0]
    assert dangling == 0
    assert world.con.execute(
        "SELECT count(*) FROM sim_hr.departments WHERE head_employee_id IS NULL"
    ).fetchone()[0] == 0


def test_payroll_totals_come_from_the_payslips(world):
    """A run's totals are summed back from its payslips, so the two can never
    disagree, and the journal entry the run names is the one that exists."""
    assert world.con.execute("""
        SELECT count(*) FROM sim_hr.payroll_runs r
        JOIN (SELECT payroll_run_id, sum(gross_pay) g, sum(net_pay) n
              FROM sim_hr.payslips GROUP BY 1) p USING (payroll_run_id)
        WHERE r.total_gross <> p.g OR r.total_net <> p.n
    """).fetchone()[0] == 0

    assert world.con.execute("""
        SELECT count(*) FROM sim_hr.payroll_runs r
        WHERE NOT EXISTS (SELECT 1 FROM sim_finance.journal_entries e
                          WHERE e.journal_entry_id = r.journal_entry_id)
    """).fetchone()[0] == 0


def test_support_tickets_reach_the_customers_and_the_orders(world):
    tickets, with_order, bad_customer = world.con.execute("""
        SELECT count(*), count(t.order_id),
               count(*) FILTER (WHERE c.customer_id IS NULL)
        FROM sim_support.tickets t
        LEFT JOIN sim_customer.customers c USING (customer_id)
    """).fetchone()
    assert tickets > 1000
    assert bad_customer == 0
    assert with_order > 0, "sales was stubbed, so some tickets should name an order"

    # Service levels are inherited: some leaves carry none of their own.
    assert world.con.execute(
        "SELECT count(*) FROM sim_support.ticket_categories "
        "WHERE parent_category_id IS NOT NULL AND sla_resolution_hours IS NULL"
    ).fetchone()[0] > 0

    assert world.con.execute("""
        SELECT count(*) FROM sim_support.ticket_messages m
        WHERE NOT EXISTS (SELECT 1 FROM sim_support.tickets t
                          WHERE t.ticket_id = m.ticket_id)
    """).fetchone()[0] == 0


def test_money_is_decimal_and_never_a_float(world):
    """DEC(18,4) upstream. Integer minor units are an extract convention, and
    a float is never either."""
    floats = world.con.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema LIKE 'sim_%'
          AND data_type IN ('FLOAT', 'DOUBLE', 'REAL')
    """).fetchall()
    assert floats == []


def test_two_builds_make_the_same_world(tmp_path):
    """Same config, same seed, same rows — table by table, over both the row
    count and a hash of every row."""
    first = _build_world(tmp_path / "a", "first.duckdb")
    second = _build_world(tmp_path / "b", "second.duckdb")
    try:
        prints = []
        for ctx in (first, second):
            tables = ctx.con.execute("""
                SELECT table_schema, table_name FROM information_schema.tables
                WHERE table_schema IN ('sim_customer', 'sim_finance', 'sim_hr',
                                       'sim_support')
                ORDER BY 1, 2
            """).fetchall()
            prints.append({
                f"{s}.{t}": ctx.con.execute(
                    f"SELECT count(*), coalesce(sum(hash(x)), 0) FROM {s}.{t} x"
                ).fetchone()
                for s, t in tables
            })
        assert prints[0] == prints[1]
        # 6 customer, 8 HR, 3 support, and finance's 9 plus `legal_entities`.
        assert len(prints[0]) == 6 + 8 + 3 + 10, sorted(prints[0])
        assert all(count > 0 for count, _ in prints[0].values()), prints[0]
    finally:
        first.con.close()
        second.con.close()
