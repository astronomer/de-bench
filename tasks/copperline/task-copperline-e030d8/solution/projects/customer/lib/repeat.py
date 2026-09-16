"""New against repeat, a day at a time, on the three consumer channels.

The growth pack asks how many of a day's orders came from somebody who had
bought before. The question only means anything once the denominator is
stated, because a large part of the day belongs to nobody: the store and web
channels take guest checkout and the OMS writes neither a trade account nor a
loyalty id on those orders. `int_customer_lifecycle` says the same thing in
its own words — divide by the guests or exclude them, but say which.

This table says which, on the row.

**A guest order is an order without an account.** It is counted in
`order_count`, it is counted in `guest_orders`, and it is in neither
`new_member_orders` nor `repeat_member_orders`. A guest is not one shopper who
buys twice a minute in every market, and a guest is not a new shopper every
time either. There is no identity on the row, so there is no lifecycle to put
it in.

**The identity is `loyalty_id`.** The trade channel is out of the population —
a trade order always names an account, the account count is NW-214's question,
and the two id formats the re-key left behind are a crosswalk this table does
not need. Off the trade channel `customer_ref` is null on every row, so the
loyalty id is the whole of the identity here.

**One population, read twice.** The rows staging drops — the synthetic orders
and the ones the OMS soft-deleted — are dropped from the day and from the
first-order lookup both. An order that is not an order is not somebody's first
order either.

**The first order is the earliest the feed holds**, over the whole history and
not over the window somebody happens to be rebuilding. A member whose first
order was in 2024 is a repeat buyer today whatever range this run covers, and
a lookup scoped to the run's own days calls every member new.

**Same-day ties go to new.** A member whose first order date is this day is
new on this day, for every order they place on it. That rule is here so that
two rebuilds of a day, and the day either side of it, cannot disagree.

`repeat_order_share_bps` divides by the member orders, not by the day. Both
are defensible and they are 1,500 basis points apart, which is why the column
name carries the population and this sentence carries the rule.
"""

from __future__ import annotations

import datetime as dt

from include.lib import warehouse

__all__ = ["TABLE", "COLUMNS", "CHANNELS", "build_daily"]

#: The page. One row per channel per day, and the day is replaced whole.
TABLE = "marts.repeat_purchase_daily"

COLUMNS = ["ds", "channel", "order_count", "guest_orders", "member_orders",
           "new_member_orders", "repeat_member_orders", "distinct_members",
           "repeat_order_share_bps"]

#: The consumer channels. The trade channel is somebody else's question.
CHANNELS = ("store", "web", "marketplace")

_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    ds DATE NOT NULL,
    channel VARCHAR NOT NULL,
    order_count BIGINT NOT NULL,
    guest_orders BIGINT NOT NULL,
    member_orders BIGINT NOT NULL,
    new_member_orders BIGINT NOT NULL,
    repeat_member_orders BIGINT NOT NULL,
    distinct_members BIGINT NOT NULL,
    repeat_order_share_bps INTEGER
)
"""


def _sql() -> str:
    """The whole day in one statement.

    Every name is qualified onto the live warehouse: the frozen `nwv` copy
    sits first on the search path and carries `raw.orders` under the same
    name, so an unqualified read answers with last September's numbers.
    """
    orders = warehouse.qualify("raw.orders")
    channels = ", ".join(f"'{name}'" for name in CHANNELS)

    return f"""
-- The population, once. Everything below reads it, so the day and the
-- first-order lookup cannot disagree about what an order is.
WITH population AS (
    SELECT o.order_id,
           o.local_order_date AS order_date,
           o.channel,
           o.loyalty_id
    FROM {orders} o
    WHERE o.channel IN ({channels})
      AND NOT coalesce(o.is_test, false)
      AND o.deleted_at IS NULL
),

-- The earliest order the feed holds for each member, over the whole history.
-- A guest has no key and is not in here at all, which is why the join below
-- is a LEFT join: an INNER one would drop every guest order off the day.
first_order AS (
    SELECT loyalty_id, min(order_date) AS first_order_date
    FROM population
    WHERE loyalty_id IS NOT NULL
    GROUP BY 1
),

day AS (
    SELECT p.channel,
           p.order_date,
           p.loyalty_id,
           f.first_order_date
    FROM population p
    LEFT JOIN first_order f ON f.loyalty_id = p.loyalty_id
    WHERE p.order_date = ?
)

SELECT order_date                                                  AS ds,
       channel,
       count(*)                                                    AS order_count,
       count(*) FILTER (WHERE loyalty_id IS NULL)                  AS guest_orders,
       count(*) FILTER (WHERE loyalty_id IS NOT NULL)              AS member_orders,
       count(*) FILTER (WHERE loyalty_id IS NOT NULL
                          AND first_order_date = order_date)       AS new_member_orders,
       count(*) FILTER (WHERE loyalty_id IS NOT NULL
                          AND first_order_date < order_date)       AS repeat_member_orders,
       count(DISTINCT loyalty_id)                                  AS distinct_members,
       cast(round(
           cast(count(*) FILTER (WHERE loyalty_id IS NOT NULL
                                   AND first_order_date < order_date)
                AS decimal(38, 4)) * 10000
           / nullif(count(*) FILTER (WHERE loyalty_id IS NOT NULL), 0), 0
       ) AS INTEGER)                                               AS repeat_order_share_bps
FROM day
GROUP BY 1, 2
ORDER BY 2
"""


def build_daily(ds: str | dt.date, table: str = TABLE) -> int:
    """Replace one day of the page. Returns the rows written.

    A channel that took no orders that day gets no row: the day is what the
    order spine holds, and a zero row is a claim the spine does not make.
    """
    day = _as_date(ds)
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('marts')}")
        con.execute(_DDL.format(table=warehouse.qualify(table)))
        rows = con.execute(_sql(), [day]).fetchall()
        return warehouse.delete_insert(
            table, "ds", day, rows, columns=COLUMNS, con=con,
        )


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
