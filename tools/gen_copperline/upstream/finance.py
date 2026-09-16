"""`sim_finance` postings — the general ledger, and the AP and AR subledgers.

ORDER: **… → sales → ecommerce → support → finance**, last. The ledger
journals what the business did, so everything it reads has to exist first. The
reference half — fiscal periods, legal entities, the chart of accounts, the
cost centers — is `finance_reference.py`, which runs early because stores and
warehouses carry a `cost_center_id`.

⚠ **Debits equal credits on every entry.** That is not a property of the data
this module happens to have; it is how the module builds. Each source works
out its named amounts first, and the line that could disagree is derived by
subtraction from the ones that cannot, in both the transaction currency and
the base currency. A test asserts it over the whole ledger.

Six sources post here.

| source | grain | reference |
|---|---|---|
| web, marketplace and trade orders | one entry per order | `order` |
| store orders | one entry per store per day | `pos_batch` |
| returns | one entry per return | `return` |
| supplier invoices | one entry per invoice | `ap_invoice` |
| supplier payments | one entry per payment | `ap_payment` |
| payroll | one entry per run, a line per cost center | `payroll_run` |

**Only trade orders are invoiced.** Store and web orders settle at the till or
the gateway, so their entry debits cash and no `ar_invoice` row exists for
them. About a third of revenue is trade, which leaves about two thirds with no
receivable at all — any recognition number that reads `ar_invoices` as the
revenue population is wrong by construction. The trade book is `channel_id`
4, which is the same population as tender method 10, `invoice`.

**The ledger journals the order, not the tender.** It never reads
`sim_sales.payments`, and that is deliberate: a failed authorisation carries
the full order total, split tender puts two rows on one order, and an
authorised row has not been captured yet. Every one of those would move a
posting that the order header already states exactly. A cancelled order posts
nothing at all.
"""

from __future__ import annotations

from ..config import Context
from ..streams import draw, pick, uniform
from . import _common as k
from .finance_reference import (
    ACC_AP, ACC_AR, ACC_CASH, ACC_COGS, ACC_FREIGHT_OUT, ACC_INVENTORY,
    ACC_MARKETING, ACC_OCCUPANCY, ACC_RECOVERABLE_TAX, ACC_RETURNS,
    ACC_REVENUE, ACC_SALES_TAX, ACC_SHIPPING_REVENUE, ACC_WAGES,
    ACC_WAGES_PAYABLE, ACC_WITHHOLDING, periods,
)
# Entry-id bands, one per source, so no two sources can ever collide.
JE_ORDER = 1_000_000_000
JE_POS_BATCH = 2_000_000_000
JE_RETURN = 3_000_000_000
JE_AP_INVOICE = 4_000_000_000
JE_AP_PAYMENT = 5_000_000_000
JE_PAYROLL = 900_000_000        # must equal what hr.payroll_runs stamps

AP_INVOICES_PER_DAY = 40
AP_INVOICE_BASE = 700_000
AP_PAYMENT_BASE = 300_000
AR_INVOICE_BASE = 40_000
BUDGET_BASE = 110_000
DEFAULT_COST_RATIO = "0.62"     # used only when order_lines has no unit cost

# The GL holds transaction currency and a base amount beside it. Rates are
# fixed per currency here rather than read from core.exchange_rates: the rate
# that matters to the world is the one stamped on the extract rows, and the
# ledger's job in this module is to balance.
FX_RATE = {"USD": "1.00000000", "CAD": "0.74000000", "GBP": "1.27340000",
           "EUR": "1.08500000", "MXN": "0.05800000"}


def build(ctx: Context) -> None:
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_finance")
    _create(ctx)
    if k.table_exists(ctx, "sim_sales", "orders"):
        _order_entries(ctx)
        _pos_batch_entries(ctx)
        _ar_invoices(ctx)
    if k.table_exists(ctx, "sim_sales", "returns"):
        _return_entries(ctx)
    _accounts_payable(ctx)
    if k.table_exists(ctx, "sim_hr", "payroll_runs"):
        _payroll_entries(ctx)
    _budgets(ctx)


# --- shared SQL fragments -------------------------------------------------

def _fx_case(column: str = "currency_code") -> str:
    arms = " ".join(f"WHEN '{c}' THEN {r}" for c, r in FX_RATE.items())
    return f"(CASE coalesce({column}, 'USD') {arms} ELSE 1.00000000 END)::DECIMAL(18,8)"


def _entity_case(ctx: Context, currency: str, split: str, entry_date: str) -> str:
    """Which legal entity books this. The currency decides it, except that
    euro volume splits between the Irish and German entities, and that an
    entity cannot book before it began trading — GB, IE and DE opened at E4,
    Mexico in FY2026 Q1, and anything earlier stays with `CL-US`."""
    starts = {e[0]: e[5] for e in k.LEGAL_ENTITIES}
    arms = []
    for code, entity_id in (("GBP", 2), ("MXN", 5)):
        arms.append(
            f"WHEN {currency} = '{code}' AND {entry_date} >= DATE '{starts[entity_id]}' "
            f"THEN {entity_id}"
        )
    arms.append(
        f"WHEN {currency} = 'EUR' AND {entry_date} >= DATE '{starts[3]}' "
        f"THEN CASE WHEN ({split}) % 2 = 0 THEN 3 ELSE 4 END"
    )
    return f"(CASE {' '.join(arms)} ELSE 1 END)"


def _period_case(ctx: Context) -> str:
    """`entry_date` to `fiscal_period_id`, inline, so no source has to join."""
    arms = " ".join(
        f"WHEN entry_date BETWEEN DATE '{start}' AND DATE '{end}' THEN {period_id}"
        for period_id, _y, _n, _q, start, end, _s in periods(ctx.cfg)
    )
    return f"(CASE {arms} ELSE 0 END)"


def _create(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_finance.journal_entries (
            journal_entry_id BIGINT PRIMARY KEY,
            entry_number TEXT NOT NULL,
            fiscal_period_id INT NOT NULL,
            entry_date DATE NOT NULL,
            source_system TEXT NOT NULL,
            reference_type TEXT NOT NULL,
            reference_id BIGINT,
            description TEXT NOT NULL,
            entry_status TEXT NOT NULL,
            posted_at TIMESTAMP NOT NULL,
            created_by_employee_id INT,
            legal_entity_id INT NOT NULL
        )
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_finance.journal_lines (
            journal_line_id BIGINT PRIMARY KEY,
            journal_entry_id BIGINT NOT NULL,
            line_no INT NOT NULL,
            account_id INT NOT NULL,
            cost_center_id INT,
            debit_amount DECIMAL(18,4) NOT NULL,
            credit_amount DECIMAL(18,4) NOT NULL,
            currency_code TEXT NOT NULL,
            fx_rate DECIMAL(18,8) NOT NULL,
            base_amount DECIMAL(18,4) NOT NULL,
            memo TEXT
        )
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_finance.ap_invoices (
            ap_invoice_id BIGINT PRIMARY KEY,
            supplier_invoice_number TEXT NOT NULL,
            supplier_id INT,
            po_id BIGINT,
            receipt_id BIGINT,
            carrier_id INT,
            invoice_date DATE NOT NULL,
            due_date DATE NOT NULL,
            net_amount DECIMAL(18,4) NOT NULL,
            tax_amount DECIMAL(18,4) NOT NULL,
            total_amount DECIMAL(18,4) NOT NULL,
            currency_code TEXT NOT NULL,
            invoice_status TEXT NOT NULL,
            journal_entry_id BIGINT NOT NULL
        )
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_finance.ap_payments (
            ap_payment_id BIGINT PRIMARY KEY,
            ap_invoice_id BIGINT NOT NULL,
            paid_at DATE NOT NULL,
            amount DECIMAL(18,4) NOT NULL,
            currency_code TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            payment_reference TEXT NOT NULL,
            journal_entry_id BIGINT NOT NULL
        )
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_finance.ar_invoices (
            ar_invoice_id BIGINT PRIMARY KEY,
            invoice_number TEXT NOT NULL,
            customer_id BIGINT NOT NULL,
            order_id BIGINT NOT NULL,
            invoice_date DATE NOT NULL,
            due_date DATE NOT NULL,
            total_amount DECIMAL(18,4) NOT NULL,
            amount_settled DECIMAL(18,4) NOT NULL,
            currency_code TEXT NOT NULL,
            invoice_status TEXT NOT NULL,
            journal_entry_id BIGINT NOT NULL
        )
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_finance.budgets (
            budget_id BIGINT PRIMARY KEY,
            fiscal_period_id INT NOT NULL,
            cost_center_id INT NOT NULL,
            account_id INT NOT NULL,
            budget_amount DECIMAL(18,2) NOT NULL,
            forecast_amount DECIMAL(18,2) NOT NULL,
            currency_code TEXT NOT NULL,
            version INT NOT NULL,
            approved_by_employee_id INT NOT NULL
        )
    """)


# --- the sales-driven half ------------------------------------------------

def _order_totals(ctx: Context, channels: str) -> str:
    """One row per order, with every amount the entry needs already worked
    out. `net_revenue` is a plug — the bill less tax and shipping — so the
    entry balances whatever arithmetic the sales module used on the header."""
    if k.table_exists(ctx, "sim_sales", "order_lines"):
        cogs, join = "l.cogs", (
            "LEFT JOIN (SELECT order_id, sum(qty * unit_cost) AS cogs "
            "FROM sim_sales.order_lines GROUP BY 1) l USING (order_id)")
    else:
        cogs, join = f"o.grand_total * {DEFAULT_COST_RATIO}", ""
    return f"""
        SELECT o.order_id, o.customer_id, o.channel_id, o.store_id,
               o.order_datetime::DATE AS entry_date,
               coalesce(o.currency_code, 'USD') AS currency_code,
               o.grand_total::DECIMAL(18,4) AS total,
               o.tax_amount::DECIMAL(18,4) AS tax,
               o.shipping_amount::DECIMAL(18,4) AS ship,
               round(coalesce({cogs}, 0), 4)::DECIMAL(18,4) AS cogs
        FROM sim_sales.orders o
        {join}
        WHERE o.channel_id IN ({channels})
          AND o.order_status <> 'cancelled'
    """


def _order_entries(ctx: Context) -> None:
    """One entry per web, marketplace or trade order. Store orders post as a
    daily batch instead — a till does not talk to the ledger one sale at a
    time."""
    channels = f"{k.CHANNEL_WEB}, {k.CHANNEL_MARKETPLACE}, {k.CHANNEL_TRADE}"
    split = draw(ctx.seed, "'journal_entity_split'", "order_id")
    entity = _entity_case(ctx, "currency_code", split, "entry_date")

    ctx.sql(f"""
        CREATE OR REPLACE TEMP TABLE _order_entries AS
        WITH totals AS ({_order_totals(ctx, channels)}),
        priced AS (
            SELECT {JE_ORDER} + order_id AS journal_entry_id,
                   order_id, customer_id, channel_id, entry_date, currency_code,
                   total, tax, ship, cogs,
                   (total - tax - ship)::DECIMAL(18,4) AS net_revenue,
                   {_fx_case()} AS fx_rate,
                   {entity} AS legal_entity_id,
                   CASE WHEN channel_id = {k.CHANNEL_TRADE}
                        THEN {ACC_AR} ELSE {ACC_CASH} END AS settle_account,
                   {k.CC_DEPARTMENT_BASE} + CASE channel_id
                        WHEN {k.CHANNEL_WEB} THEN 33
                        WHEN {k.CHANNEL_MARKETPLACE} THEN 34
                        ELSE 28 END AS cost_center_id
            FROM totals
        )
        SELECT *, {_base_amounts()} FROM priced
    """)
    ctx.sql(f"""
        INSERT INTO sim_finance.journal_entries
        SELECT journal_entry_id, 'JE-' || journal_entry_id::VARCHAR,
               {_period_case(ctx)}, entry_date,
               CASE channel_id WHEN {k.CHANNEL_WEB} THEN 'ecom'
                               WHEN {k.CHANNEL_MARKETPLACE} THEN 'marketplace'
                               ELSE 'oms' END,
               'order', order_id,
               CASE channel_id WHEN {k.CHANNEL_WEB} THEN 'Web order revenue recognition'
                               WHEN {k.CHANNEL_MARKETPLACE}
                                    THEN 'Marketplace order revenue recognition'
                               ELSE 'Trade order revenue recognition' END,
               'posted',
               entry_date::TIMESTAMP + INTERVAL 26 HOUR + INTERVAL 10 MINUTE,
               NULL, legal_entity_id
        FROM _order_entries
    """)
    _emit_lines(ctx, "_order_entries", _revenue_lines("'Order settlement'"))
    ctx.sql("DROP TABLE _order_entries")


def _pos_batch_entries(ctx: Context) -> None:
    """The store close batch: one entry per store per trading day."""
    split = draw(ctx.seed, "'journal_entity_split_pos'", "store_id")
    entity = _entity_case(ctx, "currency_code", split, "entry_date")
    ctx.sql(f"""
        CREATE OR REPLACE TEMP TABLE _pos_entries AS
        WITH totals AS ({_order_totals(ctx, str(k.CHANNEL_STORE))}),
        batched AS (
            SELECT store_id, entry_date,
                   min(currency_code) AS currency_code,
                   sum(total)::DECIMAL(18,4) AS total,
                   sum(tax)::DECIMAL(18,4) AS tax,
                   sum(ship)::DECIMAL(18,4) AS ship,
                   sum(cogs)::DECIMAL(18,4) AS cogs
            FROM totals
            WHERE store_id IS NOT NULL
            GROUP BY 1, 2
        ),
        priced AS (
            SELECT {JE_POS_BATCH} + store_id::BIGINT * 1000000
                   + (entry_date - DATE '{ctx.start}') AS journal_entry_id,
                   store_id, entry_date, currency_code, total, tax, ship, cogs,
                   (total - tax - ship)::DECIMAL(18,4) AS net_revenue,
                   {_fx_case()} AS fx_rate,
                   {entity} AS legal_entity_id,
                   {ACC_CASH} AS settle_account,
                   {k.CC_STORE_BASE} + store_id AS cost_center_id
            FROM batched
        )
        SELECT *, {_base_amounts()} FROM priced
    """)
    ctx.sql(f"""
        INSERT INTO sim_finance.journal_entries
        SELECT journal_entry_id, 'JE-' || journal_entry_id::VARCHAR,
               {_period_case(ctx)}, entry_date, 'pos', 'pos_batch', store_id,
               'Store close batch revenue recognition', 'posted',
               entry_date::TIMESTAMP + INTERVAL 25 HOUR + INTERVAL 5 MINUTE,
               NULL, legal_entity_id
        FROM _pos_entries
    """)
    _emit_lines(ctx, "_pos_entries", _revenue_lines("'Till settlement'"))
    ctx.sql("DROP TABLE _pos_entries")


def _return_entries(ctx: Context) -> None:
    """A refund reverses revenue against cash. `qty` upstream is positive and
    so is `refund_amount`; the sign lives in the account, not the row."""
    split = draw(ctx.seed, "'journal_entity_split_return'", "return_id")
    ctx.sql(f"""
        CREATE OR REPLACE TEMP TABLE _return_entries AS
        SELECT {JE_RETURN} + r.return_id AS journal_entry_id,
               r.return_id, r.customer_id, r.store_id,
               r.requested_at::DATE AS entry_date,
               'USD' AS currency_code,
               r.refund_amount::DECIMAL(18,4) AS refund,
               1.00000000::DECIMAL(18,8) AS fx_rate,
               {_entity_case(ctx, "'USD'", split, "r.requested_at::DATE")}
                 AS legal_entity_id,
               coalesce({k.CC_STORE_BASE} + r.store_id,
                        {k.CC_DEPARTMENT_BASE} + 32) AS cost_center_id
        FROM sim_sales.returns r
        WHERE r.refund_amount > 0
    """)
    ctx.sql(f"""
        INSERT INTO sim_finance.journal_entries
        SELECT journal_entry_id, 'JE-' || journal_entry_id::VARCHAR,
               {_period_case(ctx)}, entry_date, 'oms', 'return', return_id,
               'Customer refund', 'posted',
               entry_date::TIMESTAMP + INTERVAL 26 HOUR, NULL, legal_entity_id
        FROM _return_entries
    """)
    _emit_lines(ctx, "_return_entries", [
        (str(ACC_RETURNS), "refund", "refund", "debit", "'Sales returns'"),
        (str(ACC_CASH), "refund", "refund", "credit", "'Refund paid'"),
    ])
    ctx.sql("DROP TABLE _return_entries")


def _ar_invoices(ctx: Context) -> None:
    """Trade orders only. The invoice carries the same journal entry the order
    posted, because the receivable is that entry's debit."""
    h = draw(ctx.seed, "'ar_invoices'", "order_id")
    ctx.sql(f"""
        INSERT INTO sim_finance.ar_invoices
        WITH invoiced AS (
            SELECT o.order_id, o.customer_id,
                   o.order_datetime::DATE AS invoice_date,
                   coalesce(o.currency_code, 'USD') AS currency_code,
                   o.grand_total::DECIMAL(18,4) AS total,
                   row_number() OVER (ORDER BY o.order_id) AS seq,
                   {h} AS h
            FROM sim_sales.orders o
            WHERE o.channel_id = {k.CHANNEL_TRADE}
              AND o.order_status <> 'cancelled'
        ), aged AS (
            SELECT *,
                   (invoice_date + INTERVAL 30 DAY)::DATE AS due_date,
                   (h) % 1000 AS settle_draw
            FROM invoiced
        )
        SELECT {AR_INVOICE_BASE} + seq,
               'AR-' || year(invoice_date)::VARCHAR || '-'
                     || ({AR_INVOICE_BASE} + seq)::VARCHAR,
               customer_id, order_id, invoice_date, due_date,
               total,
               CASE WHEN due_date > DATE '{ctx.end}' AND settle_draw < 780 THEN 0
                    WHEN settle_draw < 74 THEN 0
                    WHEN settle_draw < 205 THEN round(total / 2, 4)
                    ELSE total END::DECIMAL(18,4),
               currency_code,
               CASE WHEN due_date > DATE '{ctx.end}' AND settle_draw < 780 THEN 'open'
                    WHEN settle_draw < 74
                         THEN CASE WHEN due_date < DATE '{ctx.end}'
                                   THEN 'overdue' ELSE 'open' END
                    WHEN settle_draw < 205 THEN 'partially_paid'
                    ELSE 'paid' END,
               {JE_ORDER} + order_id
        FROM aged
    """)


# --- the sources that stand on their own ----------------------------------

def _accounts_payable(ctx: Context) -> None:
    """Supplier invoices and the payments against them.

    `po_id` and `receipt_id` stay NULL: purchase orders and goods receipts
    belong to `sim_supply_chain`, and there is no shared id contract between
    the two modules yet. The three-way-match hazard the spec describes is
    authored there, on those tables.
    """
    seed = ctx.seed
    per_day = ctx.scale(AP_INVOICES_PER_DAY)
    h = draw(seed, "'ap_invoices'", "d.ds", "g.i")
    ctx.sql(f"""
        CREATE OR REPLACE TEMP TABLE _ap AS
        WITH drawn AS (
            SELECT {AP_INVOICE_BASE}
                   + (d.ds - DATE '{ctx.start}')::BIGINT * 1000 + g.i AS ap_invoice_id,
                   d.ds AS invoice_date,
                   {h} AS h,
                   ({h}) % 100 AS kind_draw,
                   {uniform(f"({h}) // 3", k.SUPPLIER_ID_LO, k.SUPPLIER_ID_HI)}
                     AS supplier_id,
                   round((120 + ({h}) // 5 % 4800000)::DECIMAL(18,4) / 100, 4)
                     ::DECIMAL(18,4) AS net_amount
            FROM {k.day_spine(ctx)}
            CROSS JOIN range(1, {per_day + 1}) g(i)
        )
        SELECT ap_invoice_id, invoice_date, h, supplier_id, net_amount,
               kind_draw < 12 AS is_freight,
               round(net_amount * CASE WHEN (h) // 7 % 100 < 38 THEN 0.10 ELSE 0 END, 4)
                 ::DECIMAL(18,4) AS tax_amount,
               (invoice_date + (CASE (h) // 11 % 3 WHEN 0 THEN 30 WHEN 1 THEN 45
                                ELSE 60 END) * INTERVAL 1 DAY)::DATE AS due_date,
               {JE_AP_INVOICE} + ap_invoice_id AS journal_entry_id,
               1.00000000::DECIMAL(18,8) AS fx_rate
        FROM drawn
    """)
    ctx.sql(f"""
        INSERT INTO sim_finance.ap_invoices
        SELECT ap_invoice_id,
               CASE WHEN is_freight THEN 'FRT-' ELSE 'INV-' END
                    || year(invoice_date)::VARCHAR || '-' || ap_invoice_id::VARCHAR,
               CASE WHEN is_freight THEN NULL ELSE supplier_id END,
               NULL, NULL,
               CASE WHEN is_freight THEN 1 + (h) // 13 % 6 END,
               invoice_date, due_date, net_amount, tax_amount,
               (net_amount + tax_amount)::DECIMAL(18,4), 'USD',
               {pick("(h) // 17", [("'matched'", 74), ("'approved'", 14),
                                  ("'on_hold'", 7), ("'disputed'", 5)])},
               journal_entry_id
        FROM _ap
    """)

    ctx.sql(f"""
        CREATE OR REPLACE TEMP TABLE _ap_entries AS
        SELECT journal_entry_id, ap_invoice_id AS reference_id, invoice_date AS entry_date,
               'USD' AS currency_code, fx_rate,
               net_amount, tax_amount,
               (net_amount + tax_amount)::DECIMAL(18,4) AS total_amount,
               is_freight,
               CASE WHEN is_freight THEN {ACC_FREIGHT_OUT} ELSE {ACC_INVENTORY} END
                 AS charge_account,
               CASE WHEN is_freight THEN {k.CC_DEPARTMENT_BASE} + 30
                    ELSE {k.CC_DEPARTMENT_BASE} + 28 END AS cost_center_id,
               1 AS legal_entity_id,
               {uniform("(h) // 19", k.EMPLOYEE_ID_LO, k.EMPLOYEE_ID_HI)} AS clerk
        FROM _ap
    """)
    ctx.sql(f"""
        INSERT INTO sim_finance.journal_entries
        SELECT journal_entry_id, 'JE-' || journal_entry_id::VARCHAR,
               {_period_case(ctx)}, entry_date, 'ap', 'ap_invoice', reference_id,
               CASE WHEN is_freight THEN 'Carrier invoice' ELSE 'Supplier invoice' END,
               'posted', entry_date::TIMESTAMP + INTERVAL 15 HOUR, clerk, legal_entity_id
        FROM _ap_entries
    """)
    _emit_lines(ctx, "_ap_entries", [
        ("charge_account", "net_amount", "net_amount", "debit",
         "'Goods and services received'"),
        (str(ACC_RECOVERABLE_TAX), "tax_amount", "tax_amount", "debit",
         "'Recoverable tax'"),
        (str(ACC_AP), "total_amount", "total_amount", "credit",
         "'Payable to supplier'"),
    ])

    pay = draw(seed, "'ap_payments'", "ap_invoice_id")
    ctx.sql(f"""
        CREATE OR REPLACE TEMP TABLE _ap_pay AS
        SELECT ap_invoice_id - {AP_INVOICE_BASE} + {AP_PAYMENT_BASE} AS ap_payment_id,
               ap_invoice_id,
               least((due_date + (({pay}) % 16 - 3) * INTERVAL 1 DAY)::DATE,
                     DATE '{ctx.end}') AS paid_at,
               (net_amount + tax_amount)::DECIMAL(18,4) AS amount,
               'USD' AS currency_code,
               {pick(f"({pay}) // 3", [("'ach'", 72), ("'wire'", 14),
                                      ("'check'", 9), ("'card'", 5)])} AS payment_method,
               {pay} AS h
        FROM _ap
        WHERE due_date <= DATE '{ctx.end}' AND ({pay}) % 100 < 88
    """)
    ctx.sql(f"""
        INSERT INTO sim_finance.ap_payments
        SELECT ap_payment_id, ap_invoice_id, paid_at, amount, currency_code,
               payment_method,
               upper(payment_method) || '-' || ((h) % 100000000)::VARCHAR,
               {JE_AP_PAYMENT} + ap_payment_id
        FROM _ap_pay
    """)
    ctx.sql(f"""
        CREATE OR REPLACE TEMP TABLE _ap_pay_entries AS
        SELECT {JE_AP_PAYMENT} + ap_payment_id AS journal_entry_id,
               ap_payment_id AS reference_id, paid_at AS entry_date,
               currency_code, 1.00000000::DECIMAL(18,8) AS fx_rate,
               amount, {k.CC_DEPARTMENT_BASE} + 13 AS cost_center_id,
               1 AS legal_entity_id
        FROM _ap_pay
    """)
    ctx.sql(f"""
        INSERT INTO sim_finance.journal_entries
        SELECT journal_entry_id, 'JE-' || journal_entry_id::VARCHAR,
               {_period_case(ctx)}, entry_date, 'ap', 'ap_payment', reference_id,
               'Supplier payment', 'posted',
               entry_date::TIMESTAMP + INTERVAL 16 HOUR, NULL, legal_entity_id
        FROM _ap_pay_entries
    """)
    _emit_lines(ctx, "_ap_pay_entries", [
        (str(ACC_AP), "amount", "amount", "debit", "'Payable settled'"),
        (str(ACC_CASH), "amount", "amount", "credit", "'Cash out'"),
    ])
    for temp in ("_ap", "_ap_entries", "_ap_pay", "_ap_pay_entries"):
        ctx.sql(f"DROP TABLE {temp}")


def _payroll_entries(ctx: Context) -> None:
    """One entry per payroll run, with a wages line per cost center, so labour
    lands where it was worked. The entry id is the formula `hr.payroll_runs`
    already stamped on the run: 900000000 + payroll_run_id."""
    ctx.sql(f"""
        INSERT INTO sim_finance.journal_entries
        SELECT {JE_PAYROLL}::BIGINT + r.payroll_run_id,
               'JE-' || ({JE_PAYROLL}::BIGINT + r.payroll_run_id)::VARCHAR,
               r.fiscal_period_id, r.pay_date, 'payroll', 'payroll_run',
               r.payroll_run_id, 'Payroll run ' || r.payroll_run_id::VARCHAR,
               'posted', r.pay_date::TIMESTAMP + INTERVAL 20 HOUR, NULL, 1
        FROM sim_hr.payroll_runs r
    """)
    ctx.sql(f"""
        INSERT INTO sim_finance.journal_lines
        WITH by_centre AS (
            SELECT {JE_PAYROLL}::BIGINT + r.payroll_run_id AS journal_entry_id,
                   p.cost_center_id, sum(p.gross_pay)::DECIMAL(18,4) AS gross
            FROM sim_hr.payslips p
            JOIN sim_hr.payroll_runs r USING (payroll_run_id)
            GROUP BY 1, 2
        ), numbered AS (
            SELECT *, row_number() OVER (PARTITION BY journal_entry_id
                                         ORDER BY cost_center_id) AS line_no
            FROM by_centre
        ), line_counts AS (
            SELECT journal_entry_id, max(line_no) AS lines
            FROM numbered GROUP BY 1
        ), run_totals AS (
            SELECT {JE_PAYROLL}::BIGINT + r.payroll_run_id AS journal_entry_id,
                   sum(p.tax_withheld + p.other_deductions)::DECIMAL(18,4) AS withheld,
                   sum(p.net_pay)::DECIMAL(18,4) AS net
            FROM sim_hr.payslips p
            JOIN sim_hr.payroll_runs r USING (payroll_run_id)
            GROUP BY 1
        ), totals AS (
            SELECT journal_entry_id, lines, withheld, net
            FROM run_totals JOIN line_counts USING (journal_entry_id)
        )
        SELECT journal_entry_id * 1000 + line_no, journal_entry_id, line_no,
               {ACC_WAGES}, cost_center_id, gross, 0, 'USD', 1.00000000, gross,
               'Gross wages'
        FROM numbered
        UNION ALL
        SELECT journal_entry_id * 1000 + lines + 1, journal_entry_id, lines + 1,
               {ACC_WITHHOLDING}, NULL, 0, withheld, 'USD', 1.00000000, withheld,
               'Withholdings and deductions'
        FROM totals
        UNION ALL
        SELECT journal_entry_id * 1000 + lines + 2, journal_entry_id, lines + 2,
               {ACC_WAGES_PAYABLE}, NULL, 0, net, 'USD', 1.00000000, net,
               'Net pay owed'
        FROM totals
    """)


def _budgets(ctx: Context) -> None:
    """Plan and re-forecast, several versions per period.

    ⚠ Reading every version double-counts. The latest version per
    cost center, account and period is the plan.
    """
    h = draw(ctx.seed, "'budgets'", "fiscal_period_id", "cost_center_id",
             "account_id", "version")
    period_ids = [p[0] for p in periods(ctx.cfg)
                  if p[4] >= ctx.start and p[4] <= ctx.end]
    if not period_ids:
        return
    accounts = (ACC_WAGES, ACC_OCCUPANCY, ACC_MARKETING)
    versions = draw(ctx.seed, "'budget_versions'", "p.fiscal_period_id",
                    "c.cost_center_id")
    ctx.sql(f"""
        INSERT INTO sim_finance.budgets
        WITH grid AS (
            SELECT p.fiscal_period_id, c.cost_center_id, a.account_id, v.version
            FROM (SELECT unnest({period_ids}) AS fiscal_period_id) p
            CROSS JOIN (SELECT unnest({list(accounts)}) AS account_id) a
            CROSS JOIN range(1, 4) v(version)
            CROSS JOIN sim_finance.cost_centers c
            WHERE c.is_active
              AND v.version <= 1 + ({versions}) % 3
        )
        SELECT {BUDGET_BASE} + row_number() OVER (
                   ORDER BY fiscal_period_id, cost_center_id, account_id, version),
               fiscal_period_id, cost_center_id, account_id,
               round(4000 + ({h}) % 200000 + version * 500, 2)::DECIMAL(18,2),
               round((4000 + ({h}) % 200000 + version * 500) * 1.04, 2)::DECIMAL(18,2),
               'USD', version,
               {uniform(f"({h}) // 7", k.EMPLOYEE_ID_LO, k.EMPLOYEE_ID_HI)}
        FROM grid
    """)


def _base_amounts() -> str:
    """The base-currency twin of each amount on a revenue entry.

    `base_net_revenue` is a plug: the converted bill less the converted tax and
    shipping, rather than its own rounded product. Rounding three products
    independently can leave a cent between the debits and the credits, and an
    entry that balances in the transaction currency and not in the base one is
    a ledger nobody can close.
    """
    return """
        round(total * fx_rate, 4)::DECIMAL(18,4) AS base_total,
        round(tax * fx_rate, 4)::DECIMAL(18,4) AS base_tax,
        round(ship * fx_rate, 4)::DECIMAL(18,4) AS base_ship,
        (round(total * fx_rate, 4) - round(tax * fx_rate, 4)
         - round(ship * fx_rate, 4))::DECIMAL(18,4) AS base_net_revenue,
        round(cogs * fx_rate, 4)::DECIMAL(18,4) AS base_cogs
    """


def _revenue_lines(settlement_memo: str) -> list[tuple[str, str, str, str, str]]:
    """The six lines every revenue entry posts, whether it came from one web
    order or a whole day of one store's tills."""
    return [
        ("settle_account", "total", "base_total", "debit", settlement_memo),
        (str(ACC_REVENUE), "net_revenue", "base_net_revenue", "credit",
         "'Net merchandise revenue'"),
        (str(ACC_SALES_TAX), "tax", "base_tax", "credit", "'Sales tax collected'"),
        (str(ACC_SHIPPING_REVENUE), "ship", "base_ship", "credit",
         "'Shipping revenue'"),
        (str(ACC_COGS), "cogs", "base_cogs", "debit", "'Cost of goods sold'"),
        (str(ACC_INVENTORY), "cogs", "base_cogs", "credit", "'Inventory relieved'"),
    ]


def _emit_lines(ctx: Context, source: str,
                lines: list[tuple[str, str, str, str, str]]) -> None:
    """Turn one row per entry into its debit and credit lines.

    A zero amount writes no line, which is why the line numbers can have gaps
    and why `journal_line_id` is `journal_entry_id * 1000 + line_no` rather
    than a running count.
    """
    selects = []
    for line_no, (account, amount, base, side, memo) in enumerate(lines, start=1):
        debit = amount if side == "debit" else "0"
        credit = amount if side == "credit" else "0"
        selects.append(f"""
            SELECT journal_entry_id * 1000 + {line_no}, journal_entry_id, {line_no},
                   {account}, cost_center_id,
                   ({debit})::DECIMAL(18,4), ({credit})::DECIMAL(18,4),
                   currency_code, fx_rate, ({base})::DECIMAL(18,4), {memo or 'NULL'}
            FROM {source} WHERE ({amount}) <> 0 OR ({base}) <> 0
        """)
    ctx.sql("INSERT INTO sim_finance.journal_lines "
            + " UNION ALL ".join(selects))
