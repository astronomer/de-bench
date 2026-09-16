"""`sim_hr` — the people the company is made of, in eight tables.

ORDER: **core → hr → finance_reference → …**, first of the modules this file's
author owns. Nothing extracts `hr`. It exists because a store has a manager, a
purchase order has a buyer, a return has an approver and a payslip charges a
cost center, and every one of those columns has to point at a person who is
really there. `finance.py` posts the payroll journal from `payroll_runs`.

**The circular foreign key is real and the load order is the answer.**
`employees.department_id` points at `departments`, and
`departments.head_employee_id` points back. So departments load first with
every head NULL, employees load against them, and a single UPDATE then sets
the heads. Any other order needs a deferred constraint the fiction does not
have.
"""

from __future__ import annotations

import datetime as dt

from ..config import Context
from ..streams import draw, pick, uniform
from . import _common as k
from .finance_reference import periods

# (department_id, name, parent, function). Ids stay in 1..99 so the
# cost-center formula 5000 + department_id never meets 5000 + store_id.
DEPARTMENTS = [
    (1, "Executive", None, "executive"),
    (2, "Finance", 1, "finance"),
    (3, "People", 1, "hr"),
    (4, "Technology", 1, "technology"),
    (5, "Merchandising", 1, "merchandising"),
    (6, "Marketing", 1, "marketing"),
    (7, "Retail Operations", 1, "retail_ops"),
    (8, "Supply Chain", 1, "supply_chain"),
    (9, "Customer Care", 1, "customer_care"),
    (10, "Ecommerce", 5, "ecommerce"),
    (11, "Accounting", 2, "finance"),
    (12, "Financial Planning", 2, "finance"),
    (13, "Treasury", 2, "finance"),
    (14, "Talent", 3, "hr"),
    (15, "Payroll", 3, "hr"),
    (16, "Data Platform", 4, "technology"),
    (17, "Store Systems", 4, "technology"),
    (18, "Digital Engineering", 4, "technology"),
    (19, "Hardlines Buying", 5, "merchandising"),
    (20, "Outdoor Living Buying", 5, "merchandising"),
    (21, "Inventory Planning", 5, "merchandising"),
    (22, "Brand Marketing", 6, "marketing"),
    (23, "Performance Marketing", 6, "marketing"),
    (24, "Loyalty", 6, "marketing"),
    (25, "US West Stores", 7, "retail_ops"),
    (26, "US East Stores", 7, "retail_ops"),
    (27, "International Stores", 7, "retail_ops"),
    (28, "Purchasing", 8, "supply_chain"),
    (29, "Distribution Centers", 8, "supply_chain"),
    (30, "Transportation", 8, "supply_chain"),
    (31, "Contact Center", 9, "customer_care"),
    (32, "Returns and Claims", 9, "customer_care"),
    (33, "Ecommerce Merchandising", 10, "ecommerce"),
    (34, "Marketplace", 10, "ecommerce"),
]

DEPT_MARKETING = 23
DEPT_CONTACT_CENTRE = 31

# (job_title_id, title, job_family, job_level, is_hourly, min, max, site kind).
# 412 is Store Manager because the spec's sample employee, Marcus Webb, runs
# store 214 on job title 412.
JOB_TITLES = [
    (401, "Chief Executive Officer", "executive", 10, False, 400000, 700000, "corporate"),
    (402, "Chief Financial Officer", "executive", 9, False, 320000, 520000, "corporate"),
    (403, "VP Merchandising", "executive", 8, False, 240000, 360000, "corporate"),
    (404, "VP Retail Operations", "executive", 8, False, 240000, 360000, "corporate"),
    (405, "Sales Associate", "store", 1, True, 28000, 38000, "store"),
    (406, "Senior Sales Associate", "store", 2, True, 32000, 44000, "store"),
    (407, "Cashier", "store", 1, True, 27000, 35000, "store"),
    (408, "Department Lead", "store", 3, True, 36000, 48000, "store"),
    (409, "Assistant Store Manager", "store", 4, False, 52000, 68000, "store"),
    (410, "Visual Merchandiser", "store", 3, True, 34000, 46000, "store"),
    (411, "Loss Prevention Officer", "store", 3, True, 36000, 47000, "store"),
    (412, "Store Manager", "store", 5, False, 68000, 96000, "store"),
    (413, "Trade Counter Specialist", "store", 3, True, 38000, 52000, "store"),
    (414, "Key Holder", "store", 2, True, 31000, 42000, "store"),
    (415, "Warehouse Associate", "supply_chain", 1, True, 33000, 44000, "warehouse"),
    (416, "Forklift Operator", "supply_chain", 2, True, 36000, 48000, "warehouse"),
    (417, "Inventory Controller", "supply_chain", 3, False, 46000, 62000, "warehouse"),
    (418, "Shift Supervisor", "supply_chain", 4, False, 54000, 72000, "warehouse"),
    (419, "Distribution Center Manager", "supply_chain", 6, False,
     84000, 118000, "warehouse"),
    (420, "Transport Planner", "supply_chain", 3, False, 48000, 66000, "warehouse"),
    (421, "Buyer", "merchandising", 5, False, 72000, 104000, "corporate"),
    (422, "Merchandise Planner", "merchandising", 4, False, 66000, 92000, "corporate"),
    (423, "Financial Analyst", "finance", 4, False, 68000, 94000, "corporate"),
    (424, "Accountant", "finance", 3, False, 58000, 78000, "corporate"),
    (425, "Data Engineer", "technology", 5, False, 110000, 155000, "corporate"),
    (426, "Marketing Manager", "marketing", 5, False, 78000, 108000, "corporate"),
    (427, "Customer Care Agent", "customer_care", 2, True, 34000, 46000, "corporate"),
    (428, "People Partner", "hr", 4, False, 62000, 86000, "corporate"),
]

_STORE_TITLES = [t[0] for t in JOB_TITLES if t[7] == "store"]
_WAREHOUSE_TITLES = [t[0] for t in JOB_TITLES if t[7] == "warehouse"]
_CORPORATE_TITLES = [t[0] for t in JOB_TITLES if t[7] == "corporate"]
_CORPORATE_DEPTS = [d[0] for d in DEPARTMENTS if d[0] not in (25, 26, 27, 29)]

# The sample storyline's employee, pinned so the joins in the spec resolve.
MARCUS_EMPLOYEE_ID = 3391
FIRST_HIRE_DATE = dt.date(2009, 3, 14)  # the day Ray Copper opened store one
SHIFT_BOARD_DAYS = 90  # the scheduling system keeps a rolling window, not a history


def build(ctx: Context) -> None:
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_hr")
    _departments(ctx)
    _job_titles(ctx)
    _employees(ctx)
    _department_heads(ctx)
    _compensation(ctx)
    _payroll(ctx)
    _shifts(ctx)


def _departments(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_hr.departments (
            department_id INT PRIMARY KEY,
            name TEXT NOT NULL,
            parent_department_id INT,
            function TEXT NOT NULL,
            cost_center_id INT NOT NULL,
            head_employee_id INT
        )
    """)
    ctx.con.executemany(
        "INSERT INTO sim_hr.departments VALUES (?,?,?,?,?,NULL)",
        [(i, name, parent, fn, k.CC_DEPARTMENT_BASE + i)
         for i, name, parent, fn in DEPARTMENTS],
    )


def _job_titles(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_hr.job_titles (
            job_title_id INT PRIMARY KEY,
            title TEXT NOT NULL,
            job_family TEXT NOT NULL,
            job_level INT NOT NULL,
            is_hourly BOOL NOT NULL,
            min_salary DECIMAL(18,4) NOT NULL,
            max_salary DECIMAL(18,4) NOT NULL,
            currency_code TEXT NOT NULL
        )
    """)
    ctx.con.executemany(
        "INSERT INTO sim_hr.job_titles VALUES (?,?,?,?,?,?,?,'USD')",
        [(i, title, family, level, hourly, lo, hi)
         for i, title, family, level, hourly, lo, hi, _ in JOB_TITLES],
    )


def _employees(ctx: Context) -> None:
    """3,900 people, about 3,100 of them still here — the headcount chapter 01
    gives, plus the leavers whose ids still sit on old approvals."""
    seed = ctx.seed
    h = draw(seed, "'employees'", "g.employee_id")
    site = draw(seed, "'employees_site'", "g.employee_id")
    tenure = draw(seed, "'employees_hire'", "g.employee_id")
    leave = draw(seed, "'employees_leave'", "g.employee_id")
    name_h = draw(seed, "'employees_name'", "g.employee_id")
    days = (ctx.end - FIRST_HIRE_DATE).days

    kind = pick(site, [("'store'", 72), ("'warehouse'", 12), ("'corporate'", 16)])
    first = k.first_name(name_h)
    last = k.last_name(f"({name_h}) // 37")

    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_hr.employees (
            employee_id INT PRIMARY KEY,
            employee_number TEXT NOT NULL,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            work_email TEXT NOT NULL,
            hire_date DATE NOT NULL,
            termination_date DATE,
            department_id INT NOT NULL,
            job_title_id INT NOT NULL,
            manager_employee_id INT,
            store_id INT,
            warehouse_id INT,
            address_id BIGINT NOT NULL,
            employment_type TEXT NOT NULL,
            employment_status TEXT NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_hr.employees
        WITH raw AS (
            SELECT g.employee_id,
                   {kind} AS site_kind,
                   {first} AS first_name,
                   {last} AS last_name,
                   (DATE '{FIRST_HIRE_DATE}'
                    + ({tenure} % {days}) * INTERVAL 1 DAY)::DATE AS hire_date,
                   ({leave} % 1000) AS leave_draw,
                   {uniform(site, k.STORE_ID_LO, k.STORE_ID_HI)} AS store_id,
                   {uniform(site, k.WAREHOUSE_ID_LO, k.WAREHOUSE_ID_HI)} AS warehouse_id,
                   {uniform(h, k.ADDRESS_ID_LO, k.ADDRESS_ID_HI)} AS address_id,
                   {pick(h, [("'full_time'", 62), ("'part_time'", 30), ("'seasonal'", 8)])}
                     AS employment_type,
                   {pick(f"({h}) // 101", [(str(t), 1) for t in _STORE_TITLES])}
                     AS store_title,
                   {pick(f"({h}) // 103", [(str(t), 1) for t in _WAREHOUSE_TITLES])}
                     AS warehouse_title,
                   {pick(f"({h}) // 107", [(str(t), 1) for t in _CORPORATE_TITLES])}
                     AS corporate_title,
                   {pick(f"({h}) // 109", [(str(d), 1) for d in _CORPORATE_DEPTS])}
                     AS corporate_dept
            FROM range({k.EMPLOYEE_ID_LO}, {k.EMPLOYEE_ID_HI + 1}) g(employee_id)
        ), shaped AS (
            SELECT employee_id,
                   first_name, last_name, hire_date, address_id, employment_type,
                   CASE site_kind
                        WHEN 'store' THEN CASE WHEN store_id % 3 = 0 THEN 27
                                               WHEN store_id % 2 = 0 THEN 26 ELSE 25 END
                        WHEN 'warehouse' THEN 29
                        ELSE corporate_dept END AS department_id,
                   CASE site_kind WHEN 'store' THEN store_title
                                  WHEN 'warehouse' THEN warehouse_title
                                  ELSE corporate_title END AS job_title_id,
                   CASE site_kind WHEN 'store' THEN store_id END AS store_id,
                   CASE site_kind WHEN 'warehouse' THEN warehouse_id END AS warehouse_id,
                   CASE WHEN leave_draw < 205
                        THEN (hire_date + (200 + leave_draw * 4) * INTERVAL 1 DAY)::DATE
                        END AS raw_termination
            FROM raw
        )
        SELECT employee_id,
               'E-' || employee_id::VARCHAR,
               CASE WHEN employee_id = {MARCUS_EMPLOYEE_ID} THEN 'Marcus'
                    ELSE first_name END,
               CASE WHEN employee_id = {MARCUS_EMPLOYEE_ID} THEN 'Webb'
                    ELSE last_name END,
               CASE WHEN employee_id = {MARCUS_EMPLOYEE_ID} THEN 'marcus.webb@example.com'
                    ELSE {k.slug('first_name')} || '.' || {k.slug('last_name')}
                         || '@example.com' END,
               CASE WHEN employee_id = {MARCUS_EMPLOYEE_ID} THEN DATE '2021-08-16'
                    ELSE hire_date END,
               CASE WHEN employee_id = {MARCUS_EMPLOYEE_ID} THEN NULL
                    WHEN raw_termination <= DATE '{ctx.end}' THEN raw_termination END,
               CASE WHEN employee_id = {MARCUS_EMPLOYEE_ID} THEN 26 ELSE department_id END,
               CASE WHEN employee_id = {MARCUS_EMPLOYEE_ID} THEN 412 ELSE job_title_id END,
               CASE WHEN employee_id = {MARCUS_EMPLOYEE_ID} THEN 2201
                    WHEN employee_id < {k.EMPLOYEE_ID_LO + 10} THEN NULL
                    ELSE {k.EMPLOYEE_ID_LO} + (employee_id - {k.EMPLOYEE_ID_LO}) // 13 END,
               CASE WHEN employee_id = {MARCUS_EMPLOYEE_ID} THEN 214 ELSE store_id END,
               CASE WHEN employee_id = {MARCUS_EMPLOYEE_ID} THEN NULL ELSE warehouse_id END,
               address_id,
               CASE WHEN employee_id = {MARCUS_EMPLOYEE_ID} THEN 'full_time'
                    ELSE employment_type END,
               CASE WHEN employee_id != {MARCUS_EMPLOYEE_ID}
                     AND raw_termination <= DATE '{ctx.end}'
                    THEN 'terminated' ELSE 'active' END
        FROM shaped
    """)


def _department_heads(ctx: Context) -> None:
    """The second half of the circular foreign key: every department gets a
    head, chosen from its own active people, once the people exist."""
    ctx.sql("""
        UPDATE sim_hr.departments d
        SET head_employee_id = (
            SELECT min(e.employee_id) FROM sim_hr.employees e
            WHERE e.department_id = d.department_id
              AND e.termination_date IS NULL
        )
    """)


def _compensation(ctx: Context) -> None:
    """One to three effective windows per person, contiguous and open-ended at
    the top — the shape an SCD2 comp history is built from."""
    seed = ctx.seed
    h = draw(seed, "'employee_compensation'", "e.employee_id", "v.n")
    band = draw(seed, "'employee_compensation_band'", "e.employee_id")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_hr.employee_compensation (
            compensation_id BIGINT PRIMARY KEY,
            employee_id INT NOT NULL,
            effective_from DATE NOT NULL,
            effective_to DATE,
            base_salary_annual DECIMAL(18,4),
            hourly_rate DECIMAL(18,4),
            currency_code TEXT NOT NULL,
            bonus_target_pct DECIMAL(6,4) NOT NULL,
            change_reason TEXT NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_hr.employee_compensation
        WITH windows AS (
            SELECT e.employee_id, v.n,
                   j.is_hourly, j.min_salary, j.max_salary, j.job_level,
                   (e.hire_date + (v.n - 1) * INTERVAL 400 DAY)::DATE AS effective_from,
                   CASE WHEN v.n < 1 + ({band} % 3)
                        THEN (e.hire_date + v.n * INTERVAL 400 DAY
                              - INTERVAL 1 DAY)::DATE END AS effective_to,
                   round(j.min_salary + (j.max_salary - j.min_salary)
                         * ((({h}) % 1000)::DECIMAL(10,4) / 1000)
                         * (1 + 0.03 * (v.n - 1)), 4) AS annual
            FROM sim_hr.employees e
            JOIN sim_hr.job_titles j USING (job_title_id)
            CROSS JOIN range(1, 4) v(n)
            WHERE v.n <= 1 + ({band} % 3)
        )
        SELECT employee_id::BIGINT * 10 + n,
               employee_id, effective_from, effective_to,
               CASE WHEN is_hourly THEN NULL ELSE annual::DECIMAL(18,4) END,
               CASE WHEN is_hourly THEN round(annual / 2080, 4)::DECIMAL(18,4) END,
               'USD',
               (0.02 * job_level)::DECIMAL(6,4),
               CASE n WHEN 1 THEN 'hire' WHEN 2 THEN 'annual_review'
                      ELSE 'promotion' END
        FROM windows
    """)


def _payroll(ctx: Context) -> None:
    """Semi-monthly runs, a payslip per active person per run, and the run
    totals summed back from the payslips so the two can never disagree.

    `journal_entry_id` is `900000000 + payroll_run_id` by formula, because the
    journal entry is written by `finance.py`, which runs last and cannot be
    read from here.
    """
    runs = _payroll_run_rows(ctx)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_hr.payroll_runs (
            payroll_run_id INT PRIMARY KEY,
            fiscal_period_id INT NOT NULL,
            pay_date DATE NOT NULL,
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            run_status TEXT NOT NULL,
            total_gross DECIMAL(18,4) NOT NULL,
            total_net DECIMAL(18,4) NOT NULL,
            currency_code TEXT NOT NULL,
            journal_entry_id BIGINT NOT NULL
        )
    """)
    ctx.con.executemany(
        "INSERT INTO sim_hr.payroll_runs "
        "VALUES (?,?,?,?,?,'paid',0,0,'USD',?)",
        [(run_id, period_id, pay_date, start, end, 900_000_000 + run_id)
         for run_id, period_id, pay_date, start, end in runs],
    )

    seed = ctx.seed
    h = draw(seed, "'payslips'", "r.payroll_run_id", "e.employee_id")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_hr.payslips (
            payslip_id BIGINT PRIMARY KEY,
            payroll_run_id INT NOT NULL,
            employee_id INT NOT NULL,
            cost_center_id INT NOT NULL,
            regular_hours DECIMAL(18,4) NOT NULL,
            overtime_hours DECIMAL(18,4) NOT NULL,
            gross_pay DECIMAL(18,4) NOT NULL,
            tax_withheld DECIMAL(18,4) NOT NULL,
            other_deductions DECIMAL(18,4) NOT NULL,
            net_pay DECIMAL(18,4) NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_hr.payslips
        WITH earned AS (
            SELECT r.payroll_run_id, e.employee_id,
                   coalesce({k.CC_STORE_BASE} + e.store_id,
                            {k.CC_WAREHOUSE_BASE} + e.warehouse_id,
                            {k.CC_DEPARTMENT_BASE} + e.department_id) AS cost_center_id,
                   CASE e.employment_type WHEN 'full_time' THEN 80.0
                        WHEN 'part_time' THEN 48.0 ELSE 60.0 END AS regular_hours,
                   CASE WHEN j.is_hourly AND ({h}) % 100 < 22
                        THEN 1.0 + ({h}) % 7 ELSE 0.0 END AS overtime_hours,
                   j.is_hourly, c.base_salary_annual, c.hourly_rate
            FROM sim_hr.payroll_runs r
            JOIN sim_hr.employees e
              ON e.hire_date <= r.period_end
             AND (e.termination_date IS NULL OR e.termination_date > r.period_end)
            JOIN sim_hr.job_titles j USING (job_title_id)
            JOIN sim_hr.employee_compensation c
              ON c.employee_id = e.employee_id
             AND c.effective_from <= r.period_end
             AND (c.effective_to IS NULL OR c.effective_to >= r.period_end)
        ), paid AS (
            SELECT *,
                   round(CASE WHEN is_hourly
                              THEN hourly_rate * (regular_hours + 1.5 * overtime_hours)
                              ELSE base_salary_annual / 24 END, 4) AS gross_pay
            FROM earned
        )
        SELECT payroll_run_id::BIGINT * 100000 + employee_id,
               payroll_run_id, employee_id, cost_center_id,
               regular_hours::DECIMAL(18,4), overtime_hours::DECIMAL(18,4),
               gross_pay::DECIMAL(18,4),
               round(gross_pay * 0.22, 4)::DECIMAL(18,4),
               round(gross_pay * 0.031, 4)::DECIMAL(18,4),
               (gross_pay - round(gross_pay * 0.22, 4)
                          - round(gross_pay * 0.031, 4))::DECIMAL(18,4)
        FROM paid
    """)
    ctx.sql("""
        UPDATE sim_hr.payroll_runs r
        SET total_gross = t.gross, total_net = t.net
        FROM (SELECT payroll_run_id, sum(gross_pay) AS gross, sum(net_pay) AS net
              FROM sim_hr.payslips GROUP BY 1) t
        WHERE t.payroll_run_id = r.payroll_run_id
    """)


def _payroll_run_rows(ctx: Context) -> list[tuple]:
    """Two runs a month: the 1st to the 15th paid on the 17th, and the 16th to
    month end paid on the 2nd of the next month. `payroll_run_id` is the last
    two digits of the year followed by the run's number within that year, so
    the seventh run of 2026 — 1 to 15 April, paid 17 April — is 2607."""
    period_of = {}
    for period_id, _year, _no, _q, start, end, _status in periods(ctx.cfg):
        for offset in range((end - start).days + 1):
            period_of[start + dt.timedelta(days=offset)] = period_id

    out, year, index = [], None, 0
    month = dt.date(ctx.start.year, ctx.start.month, 1)
    while month <= ctx.end:
        if month.year != year:
            year, index = month.year, 0
        next_month = dt.date(month.year + month.month // 12,
                             month.month % 12 + 1, 1)
        halves = [
            (month, month.replace(day=15), month.replace(day=17)),
            (month.replace(day=16), next_month - dt.timedelta(days=1),
             next_month + dt.timedelta(days=1)),
        ]
        for start, end, pay_date in halves:
            index += 1
            if not (ctx.start <= pay_date <= ctx.end):
                continue
            out.append(((month.year % 100) * 100 + index,
                        period_of.get(pay_date, 0), pay_date, start, end))
        month = next_month
    return out


def _shifts(ctx: Context) -> None:
    """The scheduling board, and who actually clocked in against it.

    The board is a rolling ninety days rather than the whole range: a
    workforce system keeps the roster it still needs, and the history is the
    payslip.
    """
    seed = ctx.seed
    first = ctx.end - dt.timedelta(days=SHIFT_BOARD_DAYS - 1)
    per_day = ctx.scale(2 * (k.STORE_ID_HI - k.STORE_ID_LO + 1))
    h = draw(seed, "'shifts'", "d.ds", "g.i")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_hr.shifts (
            shift_id BIGINT PRIMARY KEY,
            store_id INT,
            warehouse_id INT,
            shift_date DATE NOT NULL,
            start_time TIME NOT NULL,
            end_time TIME NOT NULL,
            role_required TEXT NOT NULL,
            required_headcount INT NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_hr.shifts
        SELECT (d.ds - DATE '{first}')::BIGINT * 100000 + g.i,
               CASE WHEN ({h}) % 100 < 88
                    THEN {uniform(h, k.STORE_ID_LO, k.STORE_ID_HI)} END,
               CASE WHEN ({h}) % 100 >= 88
                    THEN {uniform(h, k.WAREHOUSE_ID_LO, k.WAREHOUSE_ID_HI)} END,
               d.ds,
               (TIME '06:00:00' + (({h}) // 7 % 3) * INTERVAL 4 HOUR),
               (TIME '06:00:00' + (({h}) // 7 % 3) * INTERVAL 4 HOUR
                                + INTERVAL 8 HOUR),
               {pick(f"({h}) // 11", [("'sales_floor'", 40), ("'checkout'", 24),
                                     ("'trade_counter'", 12), ("'pick_pack'", 14),
                                     ("'goods_in'", 10)])},
               1 + ({h}) // 13 % 4
        FROM (SELECT ds::DATE AS ds FROM generate_series(
                  DATE '{first}', DATE '{ctx.end}', INTERVAL 1 DAY) t(ds)) d
        CROSS JOIN range(1, {per_day + 1}) g(i)
    """)

    a = draw(seed, "'shift_assignments'", "s.shift_date", "s.shift_id", "v.n")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_hr.shift_assignments (
            assignment_id BIGINT PRIMARY KEY,
            shift_id BIGINT NOT NULL,
            employee_id INT NOT NULL,
            assignment_status TEXT NOT NULL,
            clock_in TIMESTAMP,
            clock_out TIMESTAMP,
            hours_worked DECIMAL(18,4),
            overtime_hours DECIMAL(18,4),
            register_id INT
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_hr.shift_assignments
        WITH assigned AS (
            SELECT s.shift_id, s.shift_date, s.start_time, s.store_id, v.n,
                   {uniform(a, k.EMPLOYEE_ID_LO, k.EMPLOYEE_ID_HI)} AS employee_id,
                   {pick(a, [("'worked'", 88), ("'no_show'", 4),
                             ("'swapped'", 5), ("'scheduled'", 3)])} AS status,
                   ({a}) % 17 AS late_minutes
            FROM sim_hr.shifts s
            CROSS JOIN range(1, 4) v(n)
            WHERE v.n <= s.required_headcount
        )
        SELECT shift_id * 10 + n, shift_id, employee_id, status,
               CASE WHEN status = 'worked'
                    THEN shift_date + start_time
                         + late_minutes * INTERVAL 1 MINUTE END,
               CASE WHEN status = 'worked'
                    THEN shift_date + start_time
                         + INTERVAL 8 HOUR + late_minutes * INTERVAL 1 MINUTE END,
               CASE WHEN status = 'worked' THEN 8.0::DECIMAL(18,4) END,
               CASE WHEN status = 'worked' AND late_minutes > 14
                    THEN 1.0::DECIMAL(18,4) ELSE 0.0::DECIMAL(18,4) END,
               CASE WHEN store_id IS NOT NULL THEN store_id * 10 + n END
        FROM assigned
    """)
