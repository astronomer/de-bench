"""Id spaces and small SQL helpers the upstream modules share.

**The id spaces are a contract between modules, not a private choice.** The
upstream model has circular foreign keys across schemas — a store has a
manager and a cost center, an employee has a store, a cost center has a
department whose head is an employee — so a module cannot always read the
table it points at. Where it cannot, it draws from the range below and the
owning module fills that range. The ranges are decreed once, here, and the
values the spec's sample storyline uses all sit inside them: store 214,
warehouse 7, address 7712045, customer 90412, employee 3391.

Two id formulas carry the same weight, for the same reason:

    finance.cost_centers.cost_center_id = 5000 + store_id        (stores)
                                        = 5000 + department_id   (departments)
                                        = 5400 + warehouse_id    (warehouses)
    finance.journal_entries.journal_entry_id = 900000000 + payroll_run_id

Department ids stay in 1..99 and store ids in 101..368 so the two 5000+
branches never meet.
"""

from __future__ import annotations

from ..config import Context

# --- id spaces owned elsewhere, drawn from here ---------------------------
STORE_ID_LO, STORE_ID_HI = 101, 368          # 268 stores; 325..368 ex-Northwave
WAREHOUSE_ID_LO, WAREHOUSE_ID_HI = 1, 12
ADDRESS_ID_LO, ADDRESS_ID_HI = 7_700_000, 7_999_999
DISTRICT_REGION_LO, DISTRICT_REGION_HI = 3300, 3399
SUPPLIER_ID_LO, SUPPLIER_ID_HI = 101, 260
CHANNEL_STORE, CHANNEL_WEB, CHANNEL_MARKETPLACE, CHANNEL_TRADE = 1, 2, 3, 4

# --- id spaces owned here -------------------------------------------------
TRADE_CUSTOMER_BASE = 70_000     # customer_id = 70000 + trade_seq, 1..4000
CONSUMER_CUSTOMER_BASE = 90_000  # customer_id = 90000 + consumer_seq
PRIYA_SEQ = 412                  # so the storyline consumer is customer 90412
PRIYA_CUSTOMER_ID = CONSUMER_CUSTOMER_BASE + PRIYA_SEQ
PRIYA_LOYALTY_ACCOUNT_ID = 44_018
# ... and so the storyline loyalty account is 44018 on that customer.
LOYALTY_ACCOUNT_BASE = PRIYA_LOYALTY_ACCOUNT_ID - PRIYA_SEQ

EMPLOYEE_ID_LO, EMPLOYEE_ID_HI = 1001, 4900  # 3,900 rows, ~3,100 of them active
JOB_TITLE_ID_LO = 401

# --- cost-center bands ----------------------------------------------------
CC_DEPARTMENT_BASE = 5000
CC_STORE_BASE = 5000
CC_WAREHOUSE_BASE = 5400

# --- legal entities -------------------------------------------------------
# (id, code, name, country, functional currency, the date it began trading)
LEGAL_ENTITIES = [
    (1, "CL-US", "Copperline Retail Group, Inc.", "US", "USD", "2009-03-14"),
    (2, "CL-GB", "Copperline Retail UK Ltd", "GB", "GBP", "2025-04-07"),
    (3, "CL-IE", "Copperline Retail Ireland DAC", "IE", "EUR", "2025-04-07"),
    (4, "CL-DE", "Copperline Retail Deutschland GmbH", "DE", "EUR", "2025-04-07"),
    (5, "CL-MX", "Copperline Retail Mexico S. de R.L.", "MX", "MXN", "2026-02-01"),
]

_FIRST = [
    "Priya", "Marcus", "Ana", "Devon", "Leah", "Tobias", "Rosa", "Ibrahim",
    "Nadia", "Callum", "Mei", "Owen", "Sofia", "Jonas", "Amara", "Felix",
    "Clara", "Rafael", "Iris", "Hugo", "Naomi", "Simon", "Elena", "Kofi",
    "Wren", "Aditya", "Greta", "Luca", "Saoirse", "Mateo", "Ingrid", "Yusuf",
]
_LAST = [
    "Raman", "Webb", "Okafor", "Lindqvist", "Marchetti", "Bauer", "Delgado",
    "Novak", "Ferreira", "Hartley", "Nakamura", "Dubois", "Kowalski", "Reyes",
    "Sandoval", "Brennan", "Vogel", "Mbeki", "Halvorsen", "Costa", "Fitzgerald",
    "Ivanov", "Sorensen", "Adeyemi", "Whitfield", "Ortega", "Kaur", "Lombardi",
]


def sql_list(values: list[str]) -> str:
    quoted = ", ".join("'" + v.replace("'", "''") + "'" for v in values)
    return f"[{quoted}]"


def first_name(h: str) -> str:
    """A first name from a draw. DuckDB list indexing is one-based."""
    return f"{sql_list(_FIRST)}[1 + ({h}) % {len(_FIRST)}]"


def last_name(h: str) -> str:
    return f"{sql_list(_LAST)}[1 + ({h}) % {len(_LAST)}]"


def slug(expr: str) -> str:
    """An email-safe lowercase form of a name expression."""
    return f"lower(regexp_replace({expr}, '[^A-Za-z]', '', 'g'))"


def table_exists(ctx: Context, schema: str, table: str) -> bool:
    """Whether a module earlier in ORDER has built a table this one links to.

    Modules that link across schemas guard on this so that the tree still
    builds while the other module is being written, and so that a module can
    be exercised on its own in a test. The answer is fixed by ORDER, so the
    build stays deterministic either way.
    """
    return bool(ctx.con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?", [schema, table]
    ).fetchone())


def day_spine(ctx: Context, alias: str = "d") -> str:
    """Every date in the fixture range, as a subquery: `<spine> d` gives d.ds."""
    return (
        f"(SELECT ds::DATE AS ds FROM generate_series("
        f"DATE '{ctx.start}', DATE '{ctx.end}', INTERVAL 1 DAY) t(ds)) {alias}"
    )
