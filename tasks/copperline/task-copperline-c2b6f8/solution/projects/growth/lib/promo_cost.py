"""What each promotion took off our orders, a day at a time.

`marts.promo_cost_daily` is one row per promotion per day, built straight from
the OMS promotion feed. It answers one question — what did this promotion cost
us — and it is not the same question `marts.dim_promotion` answers.

**Why it does not come off the promotion dimension.** `dim_promotion` and
`agg_promo_performance` both take their `discount_cents` from
`int_promo_exposure`, and that model starts from what a promotion was allowed
to take: it joins the register to the orders inside the promotion's window, in
one of its markets, over its minimum, and then hangs the applications off that
join. An application the register cannot account for never reaches the model,
and its money leaves with it. That is the right shape for take-up, which is
what the model was built for. It is the wrong shape for cost, because the money
went out of the door whatever the register says about it.

**The feed restates money, it never adds any.** A row of
`raw.promo_applications` with no `order_line_id` carries the whole of the
order's `order_discount_cents`; a row with one carries that line's
`line_discount_cents`. So the day's applications add up to the day's
order-level discounts plus its line-level discounts, exactly, and adding the
applications to either of those columns counts the same cents twice.

**The population is `stg_sales__orders`'s.** Staff test orders carry real money
nobody owes, and the OMS deletes rather than cancels. Both are dropped in that
view and nowhere else, so a page built off `raw.orders` alone is high by both.
"""

from __future__ import annotations

from include.lib import warehouse

__all__ = ["COLUMNS", "build_daily"]

#: The page's columns, in the order the review pack reads them.
COLUMNS = ("ds", "promo_id", "promo_code", "orders", "applications",
           "header_cents", "line_cents", "discount_cents")

#: One row per promotion that took money on the day.
#:
#: `orders` counts orders and `applications` counts applications: two lines of
#: one order can take the same promotion, so the two numbers differ on nearly
#: every row and a `count(*)` under the `orders` name overstates take-up.
#:
#: Every name is qualified, at call time rather than at import time. The frozen
#: `nwv` book sits first on the search path and carries `raw.orders` under the
#: same name — see `include/lib/warehouse.py` — so an unqualified read here
#: would answer with last September's numbers.
DAY = """
SELECT a.promo_id,
       p.promo_code,
       count(DISTINCT a.order_id)::BIGINT AS orders,
       count(*)::BIGINT                   AS applications,
       sum(CASE WHEN a.order_line_id IS NULL
                THEN a.discount_cents ELSE 0 END)::BIGINT AS header_cents,
       sum(CASE WHEN a.order_line_id IS NOT NULL
                THEN a.discount_cents ELSE 0 END)::BIGINT AS line_cents,
       sum(a.discount_cents)::BIGINT      AS discount_cents
FROM {applications} a
JOIN {orders} o ON o.order_id = a.order_id
JOIN {promotions} p ON p.promo_id = a.promo_id
WHERE o.local_order_date = ?
  AND NOT coalesce(o.is_test, false)
  AND o.deleted_at IS NULL
GROUP BY 1, 2
ORDER BY 1
"""


def build_daily(target_ds: str, table: str) -> int:
    """Replace one day of `marts.promo_cost_daily`. Returns the rows written.

    The write is `delete_insert`, so the day is owned rather than appended to
    and a second run of a night leaves the same day behind it.
    """
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('marts')}")
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {warehouse.qualify(table)} ("
            "ds DATE, promo_id VARCHAR, promo_code VARCHAR, orders BIGINT, "
            "applications BIGINT, header_cents BIGINT, line_cents BIGINT, "
            "discount_cents BIGINT)"
        )
        rows = con.execute(
            DAY.format(
                applications=warehouse.qualify("raw.promo_applications"),
                orders=warehouse.qualify("raw.orders"),
                promotions=warehouse.qualify("raw.promotions"),
            ),
            [target_ds],
        ).fetchall()
        return warehouse.delete_insert(
            table, "ds", target_ds,
            [(target_ds, *row) for row in rows],
            columns=list(COLUMNS),
            con=con,
        )
