"""Folding Halyard's change feed onto the account book, and checking it.

`raw.customers` is one row per trade account, current. The API sends changes,
so the intake keeps an inbox of every version it has been given and this
module folds the newest version of each account onto the book. Keeping the
inbox is what makes a re-run harmless: merging the same changes twice leaves
one row per account.

Consumer accounts are not here. Copperline's dedup problem is a trade-account
problem, and a consumer order carries an anonymous loyalty id and never
enters this table. Anything that resolves a `C-` code against the book and
finds nothing has probably found a consumer.

The acquired book is `raw.nwv_accounts` and is loaded once, by
`cus_northwave_accounts_intake`. Nothing here touches it.
"""

from __future__ import annotations

from include.lib import warehouse

__all__ = ["BOOK", "merge_inbox", "book_problems", "book_counts"]

#: The book this module maintains.
BOOK = "raw.customers"


def merge_inbox(inbox: str) -> int:
    """Fold the inbox onto the book. Returns the accounts changed.

    The newest version of each account by `updated_at` wins. Where two
    versions share an `updated_at`, the later row in the inbox wins, which is
    the order Halyard sent them in.
    """
    book = warehouse.qualify(BOOK)
    staged = warehouse.qualify(inbox)
    with warehouse.connect() as con:
        con.execute(f"CREATE TABLE IF NOT EXISTS {book} AS "
                    f"SELECT * FROM {staged} LIMIT 0")
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE newest AS
                SELECT * EXCLUDE (arrival) FROM (
                    SELECT s.*, row_number() OVER (
                        PARTITION BY s.customer_id
                        ORDER BY s.updated_at DESC, s.rowid DESC) AS arrival
                    FROM {staged} s
                ) WHERE arrival = 1
                """
            )
            con.execute(f"DELETE FROM {book} WHERE customer_id IN "
                        "(SELECT customer_id FROM newest)")
            changed = con.execute(
                f"INSERT INTO {book} BY NAME SELECT * FROM newest"
            ).fetchall()
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return int(changed[0][0]) if changed and changed[0] else 0


def book_problems() -> list[dict]:
    """Accounts that should not be on the book in the shape they are in.

    Two cases, both silent. An account that appears twice makes every count
    downstream too high by one. An id still in the old `CUST####` scheme
    joins to nothing, because everything downstream keys on `C-######` —
    `docs/runbooks/customer-id-migration.md` §CID-1.
    """
    book = warehouse.qualify(BOOK)
    with warehouse.connect(read_only=True) as con:
        duplicates = con.execute(
            f"SELECT customer_id, count(*) FROM {book} GROUP BY customer_id "
            "HAVING count(*) > 1 ORDER BY customer_id"
        ).fetchall()
        old_scheme = con.execute(
            f"SELECT customer_id FROM {book} WHERE customer_id LIKE 'CUST%' "
            "ORDER BY customer_id"
        ).fetchall()
    return ([{"problem": "duplicate account", "customer_id": customer,
              "rows": int(count)} for customer, count in duplicates]
            + [{"problem": "old-scheme id", "customer_id": customer}
               for (customer,) in old_scheme])


def book_counts() -> dict[str, int]:
    """Accounts on the book, and how many are active.

    The warehouse count and Halyard's own count do not agree, because deletes
    have never flowed through the sync. That is a known gap in
    `contracts/customer-360.md` and not something this check treats as a
    failure.
    """
    book = warehouse.qualify(BOOK)
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"SELECT count(*), count(*) FILTER (status = 'active'), "
            f"count(*) FILTER (legacy_id IS NOT NULL) FROM {book}"
        ).fetchone()
    return {"accounts": int(row[0]), "active": int(row[1]),
            "with_legacy_id": int(row[2])}
