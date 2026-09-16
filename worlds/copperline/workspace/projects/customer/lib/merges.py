"""The integration review: sheets in, decided pairs out.

The reviewers worked in spreadsheets. Each row is a Copperline account, a
Northwave account, the reviewer's confidence, who decided it, when, and a
note. A row is a decision either way, and only the rows decided as a match
reach `ops.merge_candidates`.

That asymmetry is the whole point of the table. A pair in it was decided to
be one customer. An account absent from it might have been reviewed and
rejected or might never have been looked at, and nothing in the estate can
tell those apart — the runbook lists it as an open item and nobody has fixed
it, because the review is not going to be run again.

`confidence` is how sure the reviewer was, not how well two strings matched.
A low-confidence row is still a decision and is loaded like any other.
"""

from __future__ import annotations

from include.lib import landing_dir, warehouse

__all__ = ["INBOX", "TABLE", "DECIDED_MATCH", "review_sheets", "load_reviews",
           "apply_decisions", "pair_problems", "decision_counts", "coverage"]

#: Where the sheets land and where the decisions go.
REVIEW_DIR = "crm/merge_review"
INBOX = "ops.merge_review_inbox"
TABLE = "ops.merge_candidates"

#: The `decision` value that means "these are one customer".
DECIDED_MATCH = "match"


def review_sheets() -> list[str]:
    """Every review sheet on disk, oldest name first."""
    root = landing_dir("crm") / "merge_review"
    return sorted(str(path) for path in root.glob("*.csv")) if root.is_dir() else []


def load_reviews(sheets: list[str]) -> int:
    """Replace the inbox with what the sheets hold. Returns the rows.

    The inbox holds every row, decided either way, because the rejected ones
    are the only record that a pair was looked at at all.
    """
    if not sheets:
        return 0
    inbox = warehouse.qualify(INBOX)
    sources = ", ".join(f"'{sheet}'" for sheet in sheets)
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(
            f"CREATE OR REPLACE TABLE {inbox} AS SELECT "
            "customer_id, nwv_account_id, decision, confidence, decided_by, "
            "decided_on, note "
            f"FROM read_csv([{sources}], header = true, union_by_name = true) "
            "ORDER BY customer_id, nwv_account_id"
        )
        rows = con.execute(f"SELECT count(*) FROM {inbox}").fetchone()
    return int(rows[0] or 0)


def apply_decisions() -> int:
    """Write the decided matches to `ops.merge_candidates`. Returns the rows.

    A full replace, because the table is the current state of the review and
    not a history of it. The rejected rows stay in the inbox and go no
    further.
    """
    table = warehouse.qualify(TABLE)
    inbox = warehouse.qualify(INBOX)
    with warehouse.connect() as con:
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(
                f"CREATE OR REPLACE TABLE {table} AS SELECT "
                "customer_id, nwv_account_id, confidence, "
                "'reviewed' AS method, decided_by, decided_on, note "
                f"FROM {inbox} WHERE decision = '{DECIDED_MATCH}' "
                "ORDER BY customer_id"
            )
            rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return int(rows[0] or 0)


def pair_problems() -> list[dict]:
    """Accounts decided into more than one pair.

    An account on either book that appears twice would be merged into two
    different customers, and the dimension would then carry the same history
    under two ids. Silent, and expensive to unpick afterwards.
    """
    table = warehouse.qualify(TABLE)
    with warehouse.connect(read_only=True) as con:
        copperline = con.execute(
            f"SELECT customer_id, count(*) FROM {table} GROUP BY customer_id "
            "HAVING count(*) > 1 ORDER BY 1"
        ).fetchall()
        northwave = con.execute(
            f"SELECT nwv_account_id, count(*) FROM {table} "
            "GROUP BY nwv_account_id HAVING count(*) > 1 ORDER BY 1"
        ).fetchall()
    return ([{"side": "copperline", "id": account, "pairs": int(count)}
             for account, count in copperline]
            + [{"side": "northwave", "id": account, "pairs": int(count)}
               for account, count in northwave])


def decision_counts() -> dict[str, int]:
    """Decided pairs, and how they split by confidence."""
    table = warehouse.qualify(TABLE)
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT confidence, count(*) FROM {table} GROUP BY confidence"
        ).fetchall()
        total = con.execute(f"SELECT count(*) FROM {table}").fetchone()
    counts = {str(confidence): int(count) for confidence, count in rows}
    counts["pairs"] = int(total[0] or 0)
    return counts


def coverage() -> dict[str, int]:
    """How much of the acquired book the review reached.

    Reported, never enforced. The review happened once and will not happen
    again, so this is a fact about the estate rather than a target.
    """
    inbox = warehouse.qualify(INBOX)
    book = warehouse.qualify("raw.nwv_accounts")
    with warehouse.connect(read_only=True) as con:
        accounts = con.execute(f"SELECT count(*) FROM {book}").fetchone()
        looked_at = con.execute(
            f"SELECT count(DISTINCT nwv_account_id) FROM {inbox}"
        ).fetchone()
    return {"acquired_accounts": int(accounts[0] or 0),
            "accounts_reviewed": int(looked_at[0] or 0)}
