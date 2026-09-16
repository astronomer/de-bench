"""Support tickets: folding the feed, and the party pair that joins them.

A ticket is a moving thing — assigned, answered, closed, reopened — so the
desk sends it several times and `raw.support_tickets` holds the latest state
of each one. The inbox keeps every version, which is what makes a re-run
harmless.

**`party_ref` and `party_source` are one key, not two columns.** Halyard's
own service desk has one kind of party, because inside Halyard there is one
kind of party. Copperline has two books, so the extract puts the book on the
row: `oms` for a Copperline account, `nwv` for an acquired one. A join on
`party_ref` alone hangs an acquired customer's tickets on whichever
Copperline account happens to share the string.

Support has no shared logic and no grain to collapse, so it goes from here
straight to the customer team's marts with no intermediate model at all.
That thin case is deliberate.
"""

from __future__ import annotations

from include.lib import warehouse

__all__ = ["TABLE", "PARTY_SOURCES", "merge_inbox", "party_problems",
           "ticket_counts"]

#: The table this module maintains.
TABLE = "raw.support_tickets"

#: The books a ticket's party can belong to, and the table each resolves in.
PARTY_SOURCES = {"oms": "raw.customers", "nwv": "raw.nwv_accounts"}


def merge_inbox(inbox: str) -> int:
    """Fold the inbox onto the table. Returns the tickets changed.

    The newest version of each ticket by `updated_at` wins. A ticket that
    reopens sends a new version, so the latest state is the current state and
    not the first one.
    """
    table = warehouse.qualify(TABLE)
    staged = warehouse.qualify(inbox)
    with warehouse.connect() as con:
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS "
                    f"SELECT * FROM {staged} LIMIT 0")
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE newest AS
                SELECT * EXCLUDE (arrival) FROM (
                    SELECT s.*, row_number() OVER (
                        PARTITION BY s.ticket_id
                        ORDER BY s.updated_at DESC, s.rowid DESC) AS arrival
                    FROM {staged} s
                ) WHERE arrival = 1
                """
            )
            con.execute(f"DELETE FROM {table} WHERE ticket_id IN "
                        "(SELECT ticket_id FROM newest)")
            changed = con.execute(
                f"INSERT INTO {table} BY NAME SELECT * FROM newest"
            ).fetchall()
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return int(changed[0][0]) if changed and changed[0] else 0


def party_problems() -> list[dict]:
    """Tickets whose party pair does not resolve against either book.

    Two shapes. A `party_source` that is neither book, and a `party_ref` the
    named book does not hold. Both mean the same thing downstream: a ticket
    that never reaches an account and quietly leaves the 360.
    """
    table = warehouse.qualify(TABLE)
    with warehouse.connect(read_only=True) as con:
        unknown_book = con.execute(
            f"SELECT DISTINCT party_source FROM {table} "
            f"WHERE party_source NOT IN ({_quoted(PARTY_SOURCES)})"
        ).fetchall()
        dangling = []
        for source, book in PARTY_SOURCES.items():
            key = "customer_id" if source == "oms" else "nwv_account_id"
            rows = con.execute(
                f"""
                SELECT t.party_ref, count(*) FROM {table} t
                LEFT JOIN {warehouse.qualify(book)} b ON b.{key} = t.party_ref
                WHERE t.party_source = ? AND b.{key} IS NULL
                GROUP BY t.party_ref ORDER BY 1
                """,
                [source],
            ).fetchall()
            dangling += [{"problem": "party not on the book",
                          "party_source": source, "party_ref": ref,
                          "tickets": int(count)} for ref, count in rows]
    return ([{"problem": "unknown party_source", "party_source": source}
             for (source,) in unknown_book] + dangling)


def ticket_counts() -> dict[str, int]:
    """Tickets held, and how they split by book and by open state."""
    table = warehouse.qualify(TABLE)
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"""
            SELECT count(*),
                   count(*) FILTER (closed_at IS NULL),
                   count(*) FILTER (party_source = 'nwv')
            FROM {table}
            """
        ).fetchone()
    return {"tickets": int(row[0]), "open": int(row[1]),
            "acquired_book": int(row[2])}


def _quoted(sources) -> str:
    return ", ".join(f"'{source}'" for source in sources)
