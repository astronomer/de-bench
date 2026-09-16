"""Marketing consent, read as a state rather than as a stream of events.

Halyard records consent as events: granted on a date, withdrawn on another,
granted again. `marts.consent_daily` is the state those events imply, one row
per account per day.

**As of the day, always.** An account that withdrew in March is withdrawn on
every day after March and granted on every day before it. Taking the newest
event and applying it to history rewrites what we were allowed to do last
year, which is the one thing a consent record must never do.

**Two values and no third.** `granted` or `withdrawn`. A third value would
pass every filter written `<> 'withdrawn'` and fail every filter written
`= 'granted'`, and two consumers would then disagree about the same account
while both looking correct.

**A deletion request outranks consent.** `deletion_pending` rides on the same
row so that growth's audience filter is one column rather than a join into
another team's queue. `docs/retention-policy.md` §RET-4.
"""

from __future__ import annotations

import datetime as dt

from include.lib import landing_dir, warehouse

__all__ = ["STATES", "INBOX", "land_events", "build_state", "apply_deletions",
           "state_problems", "state_counts"]

#: The only two states there are.
STATES = ("granted", "withdrawn")

#: Where the day's events sit before the state is derived.
INBOX = "ops.consent_event_inbox"


def land_events(ds: str | dt.date) -> int:
    """Land the day's consent events. Returns the rows.

    A day is replaced rather than added to, so a re-run of a day leaves one
    copy of its events.
    """
    day = _as_date(ds)
    source = landing_dir("crm") / f"dt={day}" / "consent_events.csv"
    inbox = warehouse.qualify(INBOX)
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {inbox} AS SELECT * FROM "
            f"read_csv('{source}', header = true, union_by_name = true) LIMIT 0"
        )
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(f"DELETE FROM {inbox} WHERE event_at::DATE = ?", [day])
            landed = con.execute(
                f"INSERT INTO {inbox} BY NAME SELECT customer_id, channel, "
                "event_type, event_at FROM "
                f"read_csv('{source}', header = true, union_by_name = true)"
            ).fetchall()
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return int(landed[0][0]) if landed and landed[0] else 0


def build_state(ds: str | dt.date) -> int:
    """The state each account's consent is in as of the day. Returns the rows.

    The newest event on or before the day wins. An account with no event has
    never been asked, and not being asked is not consent: it comes out
    `withdrawn`.
    """
    day = _as_date(ds)
    inbox = warehouse.qualify(INBOX)
    accounts = warehouse.qualify("raw.customers")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            WITH latest AS (
                SELECT customer_id, channel,
                       arg_max(event_type, event_at)  AS event_type,
                       max(event_at)                  AS event_at
                FROM {inbox}
                WHERE event_at < DATE '{day}' + INTERVAL 1 DAY
                GROUP BY customer_id, channel
            )
            SELECT c.customer_id,
                   coalesce(l.channel, 'email')       AS channel,
                   CASE WHEN l.event_type = 'granted' THEN 'granted'
                        ELSE 'withdrawn' END          AS consent_state,
                   l.event_at                         AS state_since,
                   false                              AS deletion_pending,
                   DATE '{day}'                       AS ds
            FROM {accounts} c
            LEFT JOIN latest l ON l.customer_id = c.customer_id
            ORDER BY c.customer_id, channel
            """
        ).fetchall()
        return warehouse.delete_insert(
            "marts.consent_daily", "ds", day, rows,
            columns=["customer_id", "channel", "consent_state", "state_since",
                     "deletion_pending", "ds"],
            con=con,
        )


def apply_deletions(ds: str | dt.date) -> int:
    """Flag accounts with a deletion request in flight. Returns the rows.

    A request outranks consent: the account is out of every delivery whatever
    its consent says, and it stays out once the sweep has worked it.
    """
    day = _as_date(ds)
    consent = warehouse.qualify("marts.consent_daily")
    requests = warehouse.qualify("ops.deletion_requests")
    with warehouse.connect() as con:
        con.execute(
            f"""
            UPDATE {consent} SET deletion_pending = true
            WHERE ds = DATE '{day}' AND customer_id IN (
                SELECT customer_id FROM {requests}
                WHERE requested_on <= DATE '{day}'
            )
            """
        )
        row = con.execute(
            f"SELECT count(*) FROM {consent} "
            f"WHERE ds = DATE '{day}' AND deletion_pending"
        ).fetchone()
    return int(row[0] or 0)


def state_problems(ds: str | dt.date) -> list[dict]:
    """Rows that break the grain or carry a state that is neither value."""
    day = _as_date(ds)
    table = warehouse.qualify("marts.consent_daily")
    quoted = ", ".join(f"'{state}'" for state in STATES)
    with warehouse.connect(read_only=True) as con:
        duplicates = con.execute(
            f"SELECT customer_id, channel, count(*) FROM {table} "
            f"WHERE ds = DATE '{day}' GROUP BY customer_id, channel "
            "HAVING count(*) > 1 ORDER BY 1, 2"
        ).fetchall()
        strays = con.execute(
            f"SELECT DISTINCT consent_state FROM {table} "
            f"WHERE ds = DATE '{day}' AND consent_state NOT IN ({quoted})"
        ).fetchall()
    return ([{"problem": "duplicate row", "customer_id": customer,
              "channel": channel, "rows": int(count)}
             for customer, channel, count in duplicates]
            + [{"problem": "unknown state", "consent_state": state}
               for (state,) in strays])


def state_counts(ds: str | dt.date) -> dict[str, int]:
    """How the day's consent splits, and how many are held by a request."""
    day = _as_date(ds)
    table = warehouse.qualify("marts.consent_daily")
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"""
            SELECT count(*),
                   count(*) FILTER (consent_state = 'granted'),
                   count(*) FILTER (deletion_pending)
            FROM {table} WHERE ds = DATE '{day}'
            """
        ).fetchone()
    return {"rows": int(row[0]), "granted": int(row[1]),
            "deletion_pending": int(row[2])}


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
