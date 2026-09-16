"""The acquired book: reloading it, and the two facts that never move.

Northwave Supply's account book came across as one file and one load. There
is no feed behind it, no load clock on it, and no second version of it. This
module reloads the file and checks that what came back is the book we expect.

`docs/runbooks/northwave-integration.md` is the state of the integration.
Three things from it are load-bearing here and are not this module's to
change: the two books were never merged, the `nwv` reporting schema beside
them is frozen and is not a live source, and the merge decisions live in
`ops.merge_candidates` rather than in a matcher.
"""

from __future__ import annotations

from include.lib import landing_dir, warehouse

__all__ = ["TABLE", "SOURCE", "EXPECTED_ACCOUNTS", "EXPECTED_OPEN",
           "FROZEN_ON", "BOOK_LANDED_ON", "reload_book", "book_counts",
           "id_problems", "frozen_note"]

#: The table and the file behind it.
TABLE = "raw.nwv_accounts"
SOURCE = "accounts.csv"

#: What the book holds. Fixed, because the book is fixed.
EXPECTED_ACCOUNTS = 1_400
EXPECTED_OPEN = 1_380

#: The book landed six weeks after the acquisition closed; the `nwv`
#: reporting schema beside it stopped refreshing that September.
BOOK_LANDED_ON = "2025-03-17"
FROZEN_ON = "2025-09-30"


def reload_book() -> int:
    """Replace `raw.nwv_accounts` from the archived file. Returns the rows.

    A replace of the whole table rather than of a partition: the book carries
    no load clock, so there is no column to scope a partition to. Running it
    twice leaves one copy, which is the property that matters.
    """
    table = warehouse.qualify(TABLE)
    source = landing_dir("nwv") / SOURCE
    with warehouse.connect() as con:
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(
                f"CREATE OR REPLACE TABLE {table} AS SELECT "
                "nwv_account_id, account_name, primary_contact, contact_email, "
                "phone, billing_city, billing_state, country, tax_id, "
                "opened_on, status, owner, legacy_crm_id "
                f"FROM read_csv('{source}', header = true, union_by_name = true) "
                "ORDER BY nwv_account_id"
            )
            rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return int(rows[0] or 0)


def book_counts() -> dict[str, int]:
    """Accounts on the acquired book, and how many are open."""
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"SELECT count(*), count(*) FILTER (status NOT IN ('closed')) "
            f"FROM {warehouse.qualify(TABLE)}"
        ).fetchone()
    return {"accounts": int(row[0]), "open": int(row[1])}


def id_problems() -> list[dict]:
    """Ids that are not `NWA-` ids, and ids that appear twice.

    The two books share no key space on purpose. An id here shaped like a
    Copperline id would join to a live account and hang an acquired
    customer's history on it, which is the sort of thing nobody notices until
    a total moves.
    """
    table = warehouse.qualify(TABLE)
    with warehouse.connect(read_only=True) as con:
        wrong_shape = con.execute(
            f"SELECT nwv_account_id FROM {table} "
            "WHERE nwv_account_id NOT LIKE 'NWA-%' ORDER BY 1"
        ).fetchall()
        duplicates = con.execute(
            f"SELECT nwv_account_id, count(*) FROM {table} "
            "GROUP BY nwv_account_id HAVING count(*) > 1 ORDER BY 1"
        ).fetchall()
    return ([{"problem": "not an NWA id", "nwv_account_id": account}
             for (account,) in wrong_shape]
            + [{"problem": "duplicate", "nwv_account_id": account,
                "rows": int(count)} for account, count in duplicates])


def frozen_note() -> str:
    """One line saying when this book and its reporting schema stopped."""
    return (f"the acquired book landed {BOOK_LANDED_ON} and has not refreshed; "
            f"the nwv reporting schema froze {FROZEN_ON}")
