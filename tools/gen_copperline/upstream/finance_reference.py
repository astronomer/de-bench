"""`sim_finance` reference data: the fiscal calendar the books close on, the
legal entities, the chart of accounts, and the cost centers.

ORDER: **core → hr → finance_reference → …**. It sits this early because
stores, warehouses and departments all carry a `cost_center_id`, and because
`hr.payroll_runs` and every posting need a fiscal period to land in. The
postings themselves are `finance.py`, which runs last.

The fiscal periods are the same 4-5-4 calendar `extracts/calendars.py` emits,
grouped to period grain: that module's `rows()` is pure and importable, so
there is one implementation of the retail calendar and not two. A period is
`closed` when its close date has passed, and the close date is the fifth
business day after the period ends — the rule `docs/finance-policy.md`
states. As of world today that closes FY2024 P1 through FY2026 P4 and leaves
FY2026 P5 open, which is the coverage `01-timeline.md` §4.6 pins.
"""

from __future__ import annotations

import datetime as dt

from ..config import Context
from ..extracts.calendars import rows as calendar_rows
from . import _common as k

ACCOUNTS = [
    # (account_id, code, name, type, parent, normal_balance, is_postable)
    (1, "1", "Assets", "asset", None, "debit", False),
    (2, "2", "Liabilities", "liability", None, "credit", False),
    (3, "3", "Equity", "equity", None, "credit", False),
    (4, "4", "Revenue", "revenue", None, "credit", False),
    (5, "5", "Cost of Sales", "cost_of_sales", None, "debit", False),
    (6, "6", "Operating Expense", "expense", None, "debit", False),
    (1100, "1100", "Cash and Clearing", "asset", 1, "debit", True),
    (1200, "1200", "Accounts Receivable", "asset", 1, "debit", True),
    (1300, "1300", "Merchandise Inventory", "asset", 1, "debit", True),
    (1400, "1400", "Recoverable Tax", "asset", 1, "debit", True),
    (2100, "2100", "Accounts Payable", "liability", 2, "credit", True),
    (2200, "2200", "Sales Tax Payable", "liability", 2, "credit", True),
    (2300, "2300", "Payroll Withholdings Payable", "liability", 2, "credit", True),
    (2400, "2400", "Accrued Wages Payable", "liability", 2, "credit", True),
    (3100, "3100", "Retained Earnings", "equity", 3, "credit", True),
    (4000, "4000", "Merchandise Revenue", "revenue", 4, "credit", True),
    (4100, "4100", "Shipping Revenue", "revenue", 4, "credit", True),
    (4200, "4200", "Marketplace Commission Revenue", "revenue", 4, "credit", True),
    (4900, "4900", "Sales Returns and Allowances", "revenue", 4, "debit", True),
    (5000, "5000", "Cost of Goods Sold", "cost_of_sales", 5, "debit", True),
    (5100, "5100", "Freight In", "cost_of_sales", 5, "debit", True),
    (6100, "6100", "Wages and Salaries", "expense", 6, "debit", True),
    (6200, "6200", "Occupancy", "expense", 6, "debit", True),
    (6300, "6300", "Marketing", "expense", 6, "debit", True),
    (6400, "6400", "Freight Out", "expense", 6, "debit", True),
    (6900, "6900", "Other Operating Expense", "expense", 6, "debit", True),
]

# Accounts other modules post to by name rather than by number.
ACC_CASH, ACC_AR, ACC_INVENTORY, ACC_RECOVERABLE_TAX = 1100, 1200, 1300, 1400
ACC_AP, ACC_SALES_TAX, ACC_WITHHOLDING, ACC_WAGES_PAYABLE = 2100, 2200, 2300, 2400
ACC_REVENUE, ACC_SHIPPING_REVENUE, ACC_RETURNS = 4000, 4100, 4900
ACC_COGS, ACC_FREIGHT_IN = 5000, 5100
ACC_WAGES, ACC_OCCUPANCY, ACC_MARKETING, ACC_FREIGHT_OUT = 6100, 6200, 6300, 6400


def close_date(period_end: dt.date) -> dt.date:
    """The fifth business day after a period ends — when the books close."""
    day, counted = period_end, 0
    while counted < 5:
        day += dt.timedelta(days=1)
        if day.weekday() < 5:
            counted += 1
    return day


def periods(cfg: dict) -> list[tuple]:
    """One row per fiscal period, from the same 4-5-4 calendar the extracts
    emit. Pure: `hr.py` calls it too, so payroll lands in the right period
    without either module owning a second copy of the calendar."""
    today = dt.date.fromisoformat(str(cfg["today"]))
    seen: dict[tuple[int, int], list[dt.date]] = {}
    quarter_of: dict[tuple[int, int], int] = {}
    for cal_date, fiscal_year, quarter, period, *_ in calendar_rows(cfg):
        year = int(fiscal_year.removeprefix("FY"))
        seen.setdefault((year, period), []).append(cal_date)
        quarter_of[(year, period)] = quarter
    out = []
    for (year, period), dates in sorted(seen.items()):
        start, end = min(dates), max(dates)
        status = "closed" if close_date(end) <= today else "open"
        out.append((year * 100 + period, year, period, quarter_of[(year, period)],
                    start, end, status))
    return out


def build(ctx: Context) -> None:
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_finance")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_finance.fiscal_periods (
            fiscal_period_id INT PRIMARY KEY,
            fiscal_year INT NOT NULL,
            period_no INT NOT NULL,
            quarter_no INT NOT NULL,
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            period_status TEXT NOT NULL
        )
    """)
    ctx.con.executemany(
        "INSERT INTO sim_finance.fiscal_periods VALUES (?,?,?,?,?,?,?)",
        periods(ctx.cfg),
    )

    # Not one of chapter 02's nine finance tables. The world bills through
    # five legal entities and the ledger is read at entity grain, so the
    # entity has to be a column on a journal entry and the codes have to come
    # from somewhere. See the note in finance.py on how an entry gets one.
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_finance.legal_entities (
            legal_entity_id INT PRIMARY KEY,
            entity_code TEXT NOT NULL,
            name TEXT NOT NULL,
            country_code TEXT NOT NULL,
            functional_currency TEXT NOT NULL,
            trading_from DATE NOT NULL
        )
    """)
    ctx.con.executemany(
        "INSERT INTO sim_finance.legal_entities VALUES (?,?,?,?,?,?)",
        [(i, code, name, country, ccy, dt.date.fromisoformat(since))
         for i, code, name, country, ccy, since in k.LEGAL_ENTITIES],
    )

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_finance.chart_of_accounts (
            account_id INT PRIMARY KEY,
            account_code TEXT NOT NULL,
            name TEXT NOT NULL,
            account_type TEXT NOT NULL,
            parent_account_id INT,
            normal_balance TEXT NOT NULL,
            is_postable BOOL NOT NULL
        )
    """)
    ctx.con.executemany(
        "INSERT INTO sim_finance.chart_of_accounts VALUES (?,?,?,?,?,?,?)",
        [(a, c, n, t, p, b, s) for a, c, n, t, p, b, s in ACCOUNTS],
    )

    _cost_centers(ctx)


def _cost_centers(ctx: Context) -> None:
    """One cost center per department, per store and per warehouse.

    The id is the formula in `_common`, not a surrogate, because
    `store.stores.cost_center_id` and `inventory.warehouses.cost_center_id`
    are written by modules that cannot read this table.
    """
    seed = ctx.seed
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_finance.cost_centers (
            cost_center_id INT PRIMARY KEY,
            cost_center_code TEXT NOT NULL,
            name TEXT NOT NULL,
            department_id INT,
            store_id INT,
            warehouse_id INT,
            region_id INT,
            owner_employee_id INT,
            is_active BOOL NOT NULL
        )
    """)

    if k.table_exists(ctx, "sim_hr", "departments"):
        ctx.sql(f"""
            INSERT INTO sim_finance.cost_centers
            SELECT {k.CC_DEPARTMENT_BASE} + d.department_id,
                   'CC-' || ({k.CC_DEPARTMENT_BASE} + d.department_id)::VARCHAR,
                   d.name || ' Operations',
                   d.department_id, NULL, NULL, NULL,
                   d.head_employee_id, TRUE
            FROM sim_hr.departments d
        """)

    from ..streams import draw, uniform
    store_h = draw(seed, "'cost_centers_store'", "g.store_id")
    ctx.sql(f"""
        INSERT INTO sim_finance.cost_centers
        SELECT {k.CC_STORE_BASE} + g.store_id,
               'CC-' || ({k.CC_STORE_BASE} + g.store_id)::VARCHAR,
               'Store ' || g.store_id::VARCHAR || ' Operations',
               NULL, g.store_id, NULL,
               {uniform(store_h, k.DISTRICT_REGION_LO, k.DISTRICT_REGION_HI)},
               {uniform(store_h, k.EMPLOYEE_ID_LO, k.EMPLOYEE_ID_HI)},
               TRUE
        FROM range({k.STORE_ID_LO}, {k.STORE_ID_HI + 1}) g(store_id)
    """)

    wh_h = draw(seed, "'cost_centers_warehouse'", "g.warehouse_id")
    ctx.sql(f"""
        INSERT INTO sim_finance.cost_centers
        SELECT {k.CC_WAREHOUSE_BASE} + g.warehouse_id,
               'CC-' || ({k.CC_WAREHOUSE_BASE} + g.warehouse_id)::VARCHAR,
               'Distribution Center ' || g.warehouse_id::VARCHAR,
               NULL, NULL, g.warehouse_id,
               {uniform(wh_h, k.DISTRICT_REGION_LO, k.DISTRICT_REGION_HI)},
               {uniform(wh_h, k.EMPLOYEE_ID_LO, k.EMPLOYEE_ID_HI)},
               TRUE
        FROM range({k.WAREHOUSE_ID_LO}, {k.WAREHOUSE_ID_HI + 1}) g(warehouse_id)
    """)
