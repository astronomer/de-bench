"""Publishing the 360: the view, the sync, and what must not leave.

Two surfaces. `marts.v_customer_360` is the warehouse view support query, and
`exports/service_desk/<ds>.json` is the file the service desk imports every
night. Both are built from `marts.customer_360` and both hash contact detail.

**Hashing is the boundary.** The unhashed values stay in the warehouse table,
covered by `contracts/privacy.md`; anything that crosses into the view or the
export carries the hash. `unhashed_columns` is the check that says the
boundary held, and it is a check rather than a comment because the column
list changes and somebody will add an e-mail address to it one day.

**The export is a surface a deletion request has to reach.** It is on the
list in `contracts/privacy.md` and `plat_privacy_sweep` works it. It sits
outside the warehouse and dbt cannot see it, so nothing finds it except that
list.
"""

from __future__ import annotations

import datetime as dt
import json

from include.lib import warehouse, workspace_root

__all__ = ["VIEW", "EXPORT_DIR", "HASHED_COLUMNS", "CONTACT_COLUMNS",
           "latest_360_day", "refresh_view", "sync_service_desk",
           "unhashed_columns", "published_counts"]

#: The view support read.
VIEW = "marts.v_customer_360"

#: Where the service desk picks its file up.
EXPORT_DIR = "exports/service_desk"

#: Contact columns, and the hashed name each one is published under.
CONTACT_COLUMNS = {"primary_contact_hash": "primary_contact_hash"}

#: Column names that would mean an unhashed contact detail had crossed the
#: boundary. A column whose name contains one of these and does not end
#: `_hash` is a leak.
HASHED_COLUMNS = ("email", "phone", "contact", "address")


def latest_360_day() -> str:
    """The day `marts.customer_360` holds.

    An asset-triggered run has no interval to read, so the day comes from the
    table that has just been built. A run that fires late publishes the day
    it was fired for rather than whatever day it is now.
    """
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"SELECT max(updated_at)::DATE "
            f"FROM {warehouse.qualify('marts.customer_360')}"
        ).fetchone()
    if not row or row[0] is None:
        raise RuntimeError("marts.customer_360 is empty, so there is nothing "
                           "to publish")
    return _as_date(row[0]).isoformat()


def refresh_view(ds: str | dt.date) -> str:
    """Rebuild the published view and return its name.

    A closed account leaves the view and stays in the table, per
    `docs/retention-policy.md` §RET-6: it is not dropped, it stops being
    published.
    """
    view = warehouse.qualify(VIEW)
    table = warehouse.qualify("marts.customer_360")
    with warehouse.connect() as con:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW {view} AS
            SELECT customer_id, account_name, source_book, status,
                   first_order_date, last_order_date, orders_12m,
                   net_sales_cents, open_disputes, primary_contact_hash,
                   region, updated_at
            FROM {table}
            WHERE status <> 'closed'
            """
        )
    return VIEW


def sync_service_desk(ds: str | dt.date) -> int:
    """Write the whole published table for the service desk. Returns the rows.

    A full replace every night. The write is atomic — a temporary name and a
    rename — so the desk sees yesterday's file or today's and never half of
    one.
    """
    day = _as_date(ds)
    with warehouse.connect(read_only=True) as con:
        result = con.execute(f"SELECT * FROM {warehouse.qualify(VIEW)} "
                             "ORDER BY customer_id")
        names = [description[0] for description in result.description]
        rows = [dict(zip(names, row)) for row in result.fetchall()]
    root = workspace_root() / EXPORT_DIR
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{day.isoformat()}.json"
    partial = path.with_suffix(".json.partial")
    partial.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
    partial.replace(path)
    return len(rows)


def unhashed_columns() -> list[str]:
    """Columns in the view that look like unhashed contact detail.

    A name carrying `email`, `phone`, `contact` or `address` that does not
    end `_hash`. It is a name test rather than a value test on purpose: the
    check has to fail the morning somebody adds the column, not the morning
    somebody notices.
    """
    with warehouse.connect(read_only=True) as con:
        schema, _, bare = VIEW.rpartition(".")
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, bare],
        ).fetchall()
    return [name for (name,) in rows
            if any(word in name.lower() for word in HASHED_COLUMNS)
            and not name.lower().endswith("_hash")]


def published_counts() -> dict[str, int]:
    """Accounts in the view, and how many were held back as closed.

    No day: the view is one row per account, current. The day it describes is
    on `updated_at`.
    """
    with warehouse.connect(read_only=True) as con:
        published = con.execute(
            f"SELECT count(*) FROM {warehouse.qualify(VIEW)}"
        ).fetchone()
        held = con.execute(
            f"SELECT count(*) FROM {warehouse.qualify('marts.customer_360')} "
            "WHERE status = 'closed'"
        ).fetchone()
    return {"published": int(published[0] or 0), "held_back": int(held[0] or 0)}


def _as_date(value) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
