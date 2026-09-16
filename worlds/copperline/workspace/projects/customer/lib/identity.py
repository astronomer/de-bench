"""The two account books, the crosswalk, and the dimension over both.

Copperline's book is `raw.customers`, keyed `C-######`. Northwave's is
`raw.nwv_accounts`, keyed `NWA-#####`, loaded once when the acquisition
closed and never refreshed. The two were never merged, which is why every
customer-facing model carries `source_book` and why a join across them goes
through the crosswalk rather than a name.

Three rules govern everything here, and none of them is this module's to
change.

**The merge is a decision, not a match.** `ops.merge_candidates` holds the
three hundred pairs people reviewed by hand during the integration. Take the
merge from that table. Re-deriving one by matching names, addresses or
domains produces a different set — the population was chosen because it is
hard: the same customer trades under two names, two different businesses
share a domain, and `info@` addresses and resellers are everywhere.
`docs/runbooks/northwave-integration.md` §NWI-1.

**The re-key applies by id format, not by date.** A `customer_ref` that looks
like `CUST####` goes through `raw.customer_id_map` whatever date it carries,
and a `C-######` ref is already current and is never mapped. Switching on the
order date instead gives a complete answer that is wrong by forty-five
orders. `docs/runbooks/customer-id-migration.md` §CID-2.

**Sixty legacy ids have no map row.** They churned before the migration and
were never carried across. They roll up under their own legacy id rather than
being dropped or given an invented `C-` id. §CID-3, and the `coalesce` in
`resolve_refs` is that rule.
"""

from __future__ import annotations

import datetime as dt

from include.lib import warehouse

__all__ = ["SOURCE_PRIORITY", "DIM_COLUMNS", "CUTOVER_ON", "resolve_refs",
           "orphan_refs", "post_cutover_old_refs", "resolution_counts",
           "publish_resolved", "merge_pairs", "build_dim", "dim_problems",
           "dim_counts", "contract_breaks", "book_split"]

#: The day the trade accounts were re-keyed. The store integration kept
#: writing the old scheme for two more weeks after it.
CUTOVER_ON = "2024-11-04"

#: Which source wins when two versions of an attribute carry the same
#: effective date and the same `updated_at`. `contracts/customer-360.md`
#: §CU-2 fixes the order and it is written here so that two rebuilds of one
#: day produce the same row.
SOURCE_PRIORITY = ("oms", "halyard", "northwave")

#: The published dimension's columns, in contract order.
DIM_COLUMNS = (
    "customer_id", "source_book", "account_name", "status", "market_code",
    "region_code", "country_code", "tier", "payment_terms_code",
    "primary_contact_hash", "legacy_id", "nwv_account_id", "merged_from",
    "valid_from", "updated_at",
)


def resolve_refs(ds: str | dt.date) -> int:
    """Resolve one day's order references to current account ids.

    Writes `ops.customer_ref_resolved` for the day: the order, the reference
    as it was written, and the account it belongs to. The format test is the
    rule and the `coalesce` is the orphan rule; take either one out and the
    answer is still complete and still wrong.
    """
    day = _as_date(ds)
    orders = warehouse.qualify("raw.orders")
    crosswalk = warehouse.qualify("raw.customer_id_map")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT o.order_id,
                   o.customer_ref,
                   coalesce(m.customer_id, o.customer_ref) AS customer_id,
                   m.customer_id IS NULL
                       AND o.customer_ref LIKE 'CUST%'     AS is_orphan,
                   DATE '{day}'                            AS ds
            FROM {orders} o
            LEFT JOIN {crosswalk} m
              ON o.customer_ref = m.legacy_id
             AND o.customer_ref LIKE 'CUST%'
            WHERE o.order_date = DATE '{day}' AND o.customer_ref IS NOT NULL
            ORDER BY o.order_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "ops.customer_ref_resolved", "ds", day, rows,
            columns=["order_id", "customer_ref", "customer_id", "is_orphan",
                     "ds"],
            con=con,
        )


def orphan_refs(ds: str | dt.date) -> list[dict]:
    """The day's references that went through the crosswalk and found nothing.

    Expected, not broken: sixty legacy ids have no map row and never will.
    They are listed so that a day with a new one in it is visible, because a
    legacy id surfacing now has nowhere to go.
    """
    day = _as_date(ds)
    resolved = warehouse.qualify("ops.customer_ref_resolved")
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT customer_ref, count(*) FROM {resolved} "
            f"WHERE ds = DATE '{day}' AND is_orphan GROUP BY customer_ref "
            "ORDER BY customer_ref"
        ).fetchall()
    return [{"customer_ref": ref, "orders": int(count)} for ref, count in rows]


def post_cutover_old_refs(ds: str | dt.date) -> list[dict]:
    """Old-format references on orders dated after the re-key.

    Forty-five of them, through the two weeks the store integration kept
    writing the old scheme. They are real orders from real accounts and they
    resolve correctly by format. Counted so that the number stays the number.
    """
    day = _as_date(ds)
    if day.isoformat() < CUTOVER_ON:
        return []
    resolved = warehouse.qualify("ops.customer_ref_resolved")
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT customer_ref, customer_id, count(*) FROM {resolved} "
            f"WHERE ds = DATE '{day}' AND customer_ref LIKE 'CUST%' "
            "GROUP BY customer_ref, customer_id ORDER BY customer_ref"
        ).fetchall()
    return [{"customer_ref": ref, "customer_id": customer, "orders": int(count)}
            for ref, customer, count in rows]


def resolution_counts(ds: str | dt.date) -> dict[str, int]:
    """How the day's references resolved: mapped, already current, orphaned."""
    day = _as_date(ds)
    resolved = warehouse.qualify("ops.customer_ref_resolved")
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"""
            SELECT count(*),
                   count(*) FILTER (customer_ref LIKE 'CUST%' AND NOT is_orphan),
                   count(*) FILTER (customer_ref NOT LIKE 'CUST%'),
                   count(*) FILTER (is_orphan)
            FROM {resolved} WHERE ds = DATE '{day}'
            """
        ).fetchone()
    return {"orders": int(row[0]), "mapped": int(row[1]),
            "already_current": int(row[2]), "orphaned": int(row[3])}


def publish_resolved(ds: str | dt.date) -> str:
    """Write the day's resolved references out as a partition file."""
    day = _as_date(ds)
    resolved = warehouse.qualify("ops.customer_ref_resolved")
    with warehouse.connect(read_only=True) as con:
        result = con.execute(
            f"SELECT order_id, customer_ref, customer_id, is_orphan "
            f"FROM {resolved} WHERE ds = DATE '{day}' ORDER BY order_id"
        )
        header = [description[0] for description in result.description]
        rows = result.fetchall()
    return str(warehouse.write_partition("ops", "customer_ref_resolved", day,
                                         header, rows))


def merge_pairs() -> list[dict]:
    """The reviewed merge decisions, as people made them.

    One row per decided pair, with the reviewer and the date. `confidence`
    records how sure the reviewer was, not how well two strings matched, and
    a low-confidence row is still a decision.
    """
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            "SELECT customer_id, nwv_account_id, confidence, decided_by, "
            f"decided_on FROM {warehouse.qualify('ops.merge_candidates')} "
            "ORDER BY customer_id"
        ).fetchall()
    return [{"customer_id": customer, "nwv_account_id": account,
             "confidence": confidence, "decided_by": reviewer,
             "decided_on": str(decided)}
            for customer, account, confidence, reviewer, decided in rows]


def build_dim(ds: str | dt.date) -> int:
    """Rebuild `marts.dim_customer` for one day. Returns the rows written.

    One row per real account. Where the review says two records are the same
    customer, the Copperline record survives and carries the acquired id in
    `merged_from`; where two records look alike and were not decided, both
    stay. An acquired account with no decision is its own account, on the
    `northwave` book.

    An account deleted at source is not dropped. It leaves the published view
    and stays reconstructable, per `docs/retention-policy.md` §RET-6, which
    is why `status` carries `closed` rather than the row disappearing.
    """
    day = _as_date(ds)
    copperline = warehouse.qualify("raw.customers")
    northwave = warehouse.qualify("raw.nwv_accounts")
    merges = warehouse.qualify("ops.merge_candidates")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            WITH decided AS (
                SELECT customer_id, nwv_account_id FROM {merges}
            ), survivors AS (
                SELECT c.customer_id,
                       'copperline'                            AS source_book,
                       c.account_name,
                       CASE WHEN c.deleted_at IS NOT NULL THEN 'closed'
                            WHEN c.status = 'churned' THEN 'suspended'
                            ELSE 'active' END                  AS status,
                       c.market_code,
                       c.region_code,
                       c.country_code,
                       c.tier,
                       c.payment_terms_code,
                       md5(coalesce(c.email, ''))              AS primary_contact_hash,
                       c.legacy_id,
                       d.nwv_account_id,
                       d.nwv_account_id                        AS merged_from,
                       c.created_on                            AS valid_from,
                       c.updated_at
                FROM {copperline} c
                LEFT JOIN decided d ON d.customer_id = c.customer_id
            ), acquired AS (
                SELECT n.nwv_account_id                        AS customer_id,
                       'northwave'                             AS source_book,
                       n.account_name,
                       CASE WHEN n.status = 'closed' THEN 'closed'
                            WHEN n.status = 'dormant' THEN 'suspended'
                            ELSE 'active' END                  AS status,
                       NULL                                    AS market_code,
                       n.billing_state                         AS region_code,
                       n.country                               AS country_code,
                       NULL                                    AS tier,
                       NULL                                    AS payment_terms_code,
                       md5(coalesce(n.contact_email, ''))      AS primary_contact_hash,
                       n.legacy_crm_id                         AS legacy_id,
                       n.nwv_account_id,
                       NULL                                    AS merged_from,
                       n.opened_on                             AS valid_from,
                       n.opened_on::TIMESTAMP                  AS updated_at
                FROM {northwave} n
                WHERE n.nwv_account_id NOT IN (SELECT nwv_account_id FROM decided)
            )
            SELECT * FROM survivors UNION ALL SELECT * FROM acquired
            ORDER BY source_book, customer_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "marts.dim_customer", "ds", day,
            [(*row, day) for row in rows],
            columns=[*DIM_COLUMNS, "ds"], con=con,
        )


def dim_problems(ds: str | dt.date) -> list[dict]:
    """What is wrong with the day's dimension, if anything.

    Three things, and all three are silent when they go wrong: an account
    that appears twice, an acquired account that both survived a merge and
    stayed on its own, and a `source_book` that is neither book.
    """
    day = _as_date(ds)
    dim = warehouse.qualify("marts.dim_customer")
    with warehouse.connect(read_only=True) as con:
        duplicates = con.execute(
            f"SELECT customer_id, count(*) FROM {dim} WHERE ds = DATE '{day}' "
            "GROUP BY customer_id HAVING count(*) > 1 ORDER BY customer_id"
        ).fetchall()
        both = con.execute(
            f"""
            SELECT merged.merged_from FROM {dim} merged
            JOIN {dim} standing ON standing.customer_id = merged.merged_from
                               AND standing.ds = merged.ds
            WHERE merged.ds = DATE '{day}' AND merged.merged_from IS NOT NULL
            ORDER BY 1
            """
        ).fetchall()
        books = con.execute(
            f"SELECT DISTINCT source_book FROM {dim} WHERE ds = DATE '{day}' "
            "AND source_book NOT IN ('copperline', 'northwave')"
        ).fetchall()
    problems = [{"problem": "duplicate account", "customer_id": customer,
                 "rows": int(count)} for customer, count in duplicates]
    problems += [{"problem": "merged and standing", "customer_id": account}
                 for (account,) in both]
    problems += [{"problem": "unknown source_book", "source_book": book}
                 for (book,) in books]
    return problems


def dim_counts(ds: str | dt.date) -> dict[str, int]:
    """Accounts in the day's dimension, by book and by status."""
    day = _as_date(ds)
    dim = warehouse.qualify("marts.dim_customer")
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"""
            SELECT count(*),
                   count(*) FILTER (source_book = 'copperline'),
                   count(*) FILTER (source_book = 'northwave'),
                   count(*) FILTER (status = 'active'),
                   count(*) FILTER (merged_from IS NOT NULL)
            FROM {dim} WHERE ds = DATE '{day}'
            """
        ).fetchone()
    return {"accounts": int(row[0]), "copperline": int(row[1]),
            "northwave": int(row[2]), "active": int(row[3]),
            "merged": int(row[4])}


def contract_breaks(ds: str | dt.date) -> list[str]:
    """The dimension against its contract file, as a list of sentences.

    `plat_contracts_enforce` runs the same file every morning at 06:00. It is
    run here as well, two hours earlier, so that a break is found by the team
    that caused it rather than by the platform team's alert.
    """
    from include.lib import contracts

    return [str(violation) for violation in contracts.check("dim_customer")]


def book_split(ds: str | dt.date) -> dict[str, int]:
    """How much of the acquired book stands on its own in the dimension.

    Around eleven hundred accounts. They are not errors: the books were never
    merged and only three hundred pairs were ever decided. A large move means
    a decision changed, which is worth knowing the morning it happens.
    """
    day = _as_date(ds)
    dim = warehouse.qualify("marts.dim_customer")
    book = warehouse.qualify("raw.nwv_accounts")
    with warehouse.connect(read_only=True) as con:
        standing = con.execute(
            f"SELECT count(*) FROM {dim} "
            f"WHERE ds = DATE '{day}' AND source_book = 'northwave'"
        ).fetchone()
        total = con.execute(f"SELECT count(*) FROM {book}").fetchone()
    return {"acquired_accounts": int(total[0] or 0),
            "standing_alone": int(standing[0] or 0),
            "merged_away": int((total[0] or 0) - (standing[0] or 0))}


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
