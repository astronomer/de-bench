"""What we know about an account, gathered one subject at a time.

The 360 is eight joins wide and each leg is its own question — orders,
payments, disputes, support — so each leg is its own function and its own
task. A leg that fails then names itself instead of failing a nine-minute
query somewhere in the middle.

Every leg writes to `ops.profile_<subject>`, keyed by account and day, and
the model joins them. Nothing here writes the mart: `customer_360` is a dbt
model and this is what it reads.

**Money is `net_sales_cents` as `docs/semantic-definitions.md` defines it**,
trailing twelve months, and this module reads it from
`marts.order_economics` rather than recomputing it. A second definition of a
reserved name is a defect whatever it computes.

**Both books, one account list.** `resolve_accounts` applies the reviewed
merges and produces the list every leg works over, so no leg can decide the
grain for itself.
"""

from __future__ import annotations

import datetime as dt

from include.lib import warehouse

__all__ = ["ACCOUNTS", "resolve_accounts", "order_history", "payment_history",
           "dispute_history", "support_history", "contract_breaks",
           "profile_counts"]

#: The account list every leg works over.
ACCOUNTS = "ops.profile_accounts"


def resolve_accounts(ds: str | dt.date) -> int:
    """The account list for a day, merges applied. Returns the rows.

    Taken from the dimension rather than from the books, so that the 360 and
    the dimension cannot disagree about who exists. `merged_from` travels
    with the row, which is how a leg finds the acquired account's history for
    a Copperline account.
    """
    day = _as_date(ds)
    dim = warehouse.qualify("marts.dim_customer")
    with warehouse.connect() as con:
        rows = con.execute(
            f"SELECT customer_id, source_book, merged_from, status, "
            f"account_name, region_code AS region, DATE '{day}' AS ds "
            f"FROM {dim} WHERE ds = DATE '{day}' ORDER BY customer_id"
        ).fetchall()
        return warehouse.delete_insert(
            ACCOUNTS, "ds", day, rows,
            columns=["customer_id", "source_book", "merged_from", "status",
                     "account_name", "region", "ds"],
            con=con,
        )


def order_history(ds: str | dt.date) -> int:
    """Orders, net sales and the first and last order date, per account.

    Trailing twelve months for the money, ending on the day inclusive. The
    acquired account's orders come in through `merged_from`, which is why the
    join is against the resolved list rather than against the raw refs.
    """
    day = _as_date(ds)
    accounts = warehouse.qualify(ACCOUNTS)
    resolved = warehouse.qualify("ops.customer_ref_resolved")
    economics = warehouse.qualify("marts.order_economics")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT a.customer_id,
                   count(DISTINCT e.order_id)
                       FILTER (e.order_date > DATE '{day}' - INTERVAL 12 MONTH)
                                                        AS orders_12m,
                   coalesce(sum(e.net_sales_cents)
                       FILTER (e.order_date > DATE '{day}' - INTERVAL 12 MONTH),
                       0)::BIGINT                       AS net_sales_cents,
                   min(e.order_date)                    AS first_order_date,
                   max(e.order_date)                    AS last_order_date,
                   DATE '{day}'                         AS ds
            FROM {accounts} a
            LEFT JOIN {resolved} r
              ON r.customer_id IN (a.customer_id, a.merged_from)
            LEFT JOIN {economics} e
              ON e.order_id = r.order_id AND e.order_date <= DATE '{day}'
            WHERE a.ds = DATE '{day}'
            GROUP BY a.customer_id ORDER BY a.customer_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "ops.profile_orders", "ds", day, rows,
            columns=["customer_id", "orders_12m", "net_sales_cents",
                     "first_order_date", "last_order_date", "ds"],
            con=con,
        )


def payment_history(ds: str | dt.date) -> int:
    """Terms, open balance and how long an account takes to pay."""
    day = _as_date(ds)
    accounts = warehouse.qualify(ACCOUNTS)
    invoices = warehouse.qualify("raw.invoices")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT a.customer_id,
                   coalesce(sum(i.balance_cents)
                       FILTER (i.status <> 'paid'), 0)::BIGINT AS open_cents,
                   count(*) FILTER (i.status <> 'paid')        AS open_invoices,
                   round(avg(date_diff('day', i.issued_on, i.paid_on))
                       FILTER (i.paid_on IS NOT NULL))         AS days_to_pay,
                   DATE '{day}'                                AS ds
            FROM {accounts} a
            LEFT JOIN {invoices} i
              ON i.customer_id IN (a.customer_id, a.merged_from)
             AND i.issued_on <= DATE '{day}'
            WHERE a.ds = DATE '{day}'
            GROUP BY a.customer_id ORDER BY a.customer_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "ops.profile_payments", "ds", day, rows,
            columns=["customer_id", "open_cents", "open_invoices",
                     "days_to_pay", "ds"],
            con=con,
        )


def dispute_history(ds: str | dt.date) -> int:
    """Open disputes per account, and the oldest one's age."""
    day = _as_date(ds)
    accounts = warehouse.qualify(ACCOUNTS)
    disputes = warehouse.qualify("raw.disputes")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT a.customer_id,
                   count(*) FILTER (d.closed_on IS NULL)   AS open_disputes,
                   coalesce(max(date_diff('day', d.raised_on, DATE '{day}'))
                       FILTER (d.closed_on IS NULL), 0)    AS oldest_open_days,
                   DATE '{day}'                            AS ds
            FROM {accounts} a
            LEFT JOIN {disputes} d
              ON d.customer_id IN (a.customer_id, a.merged_from)
             AND d.raised_on <= DATE '{day}'
            WHERE a.ds = DATE '{day}'
            GROUP BY a.customer_id ORDER BY a.customer_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "ops.profile_disputes", "ds", day, rows,
            columns=["customer_id", "open_disputes", "oldest_open_days", "ds"],
            con=con,
        )


def support_history(ds: str | dt.date) -> int:
    """Tickets and last contact per account, over both books' parties.

    A ticket carries `party_ref` and `party_source`, because Halyard's own
    service desk has one kind of party and Copperline has two. The pair is
    what joins a ticket to an account across the two books.
    """
    day = _as_date(ds)
    accounts = warehouse.qualify(ACCOUNTS)
    tickets = warehouse.qualify("raw.support_tickets")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT a.customer_id,
                   count(t.ticket_id)                          AS tickets,
                   count(t.ticket_id) FILTER (t.closed_at IS NULL)
                                                               AS open_tickets,
                   max(t.opened_at)                            AS last_contact_at,
                   DATE '{day}'                                AS ds
            FROM {accounts} a
            LEFT JOIN {tickets} t
              ON (t.party_source = 'oms' AND t.party_ref = a.customer_id)
              OR (t.party_source = 'nwv' AND t.party_ref = a.merged_from)
              OR (t.party_source = 'nwv' AND a.source_book = 'northwave'
                  AND t.party_ref = a.customer_id)
            WHERE a.ds = DATE '{day}' AND (t.opened_at IS NULL
                                           OR t.opened_at < DATE '{day}'
                                              + INTERVAL 1 DAY)
            GROUP BY a.customer_id ORDER BY a.customer_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "ops.profile_support", "ds", day, rows,
            columns=["customer_id", "tickets", "open_tickets",
                     "last_contact_at", "ds"],
            con=con,
        )


def contract_breaks() -> list[str]:
    """`marts.customer_360` against its contract file, as sentences."""
    from include.lib import contracts

    return [str(violation) for violation in contracts.check("customer_360")]


def profile_counts() -> dict[str, int]:
    """Accounts in the 360, and how they split by book and status.

    No day: the grain is one row per account, current, which is what
    `contracts/customer_360.yml` fixes. The day the table describes is on
    `updated_at`.
    """
    table = warehouse.qualify("marts.customer_360")
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"""
            SELECT count(*),
                   count(*) FILTER (source_book = 'northwave'),
                   count(*) FILTER (status = 'active'),
                   count(*) FILTER (orders_12m > 0)
            FROM {table}
            """
        ).fetchone()
    return {"accounts": int(row[0]), "from_acquired_book": int(row[1]),
            "active": int(row[2]), "ordered_in_12m": int(row[3])}


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
