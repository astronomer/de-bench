"""Building the two daily finance marts, and the checks that follow them.

`marts.daily_revenue` and `marts.category_margin` are the same shape: read one
day of a shared `int` model, group it, replace the day. They are siblings on
purpose — the house style for a finance daily is a full rebuild of the day, in
integer cents, with a tie at the end — and keeping both here is what stops them
drifting apart.

Neither reads `source()` and neither rebuilds a definition another team owns.
Net sales and landed cost are shared `int` models under platform ownership, and
commerce reads the same two.
"""

from __future__ import annotations

from include.lib import warehouse

__all__ = ["CHANNELS", "build_daily", "build_margin", "assert_channels"]

#: Every channel an order can arrive through. `trade` is the wholesale arm and
#: it is a third of revenue on six per cent of the orders, so a rollup that
#: quietly drops it looks nearly right.
CHANNELS = ("store", "web", "marketplace", "trade")


def build_daily(target_ds: str, table: str) -> int:
    """Replace one day of `marts.daily_revenue`. Returns the rows written.

    One row per channel. `booked_cents` is the order's value on the order date,
    gross of nothing and net of nothing — `docs/semantic-definitions.md` fixes
    that meaning and it is not the recognised figure.

    The source is `int_orders_enriched`, the shared model commerce and growth
    also read. Finance does not rebuild the order spine.

    **The channel is the order's own and nothing else decides it.** The store
    that rang a sale says where it was rung, not what it was: an acquired store
    sells over the counter the same way ours do, and re-reading the selling
    store to move those sales into `trade` moves money between two reserved
    names while leaving the day's total exactly right. Every check on this DAG
    passes on that — the total ties, the cents are integers, all four channels
    have a row — so the only thing that shows it is the split itself.
    """
    with warehouse.connect() as con:
        rows = con.execute(
            f"""SELECT channel,
                       sum(booked_cents)::BIGINT AS booked_cents,
                       count(*)::BIGINT         AS order_count
                FROM {warehouse.qualify('int.int_orders_enriched')}
                WHERE order_date = ?
                GROUP BY channel
                ORDER BY channel""",
            [target_ds],
        ).fetchall()
        return warehouse.delete_insert(
            table, "ds", target_ds,
            [(target_ds, channel, booked, orders) for channel, booked, orders in rows],
            columns=["ds", "channel", "booked_cents", "order_count"],
            con=con,
        )


def build_margin(target_ds: str, table: str) -> int:
    """Replace one day of `marts.category_margin`. Returns the rows written.

    Net sales less landed cost, by category, in cents. The join to
    `int_order_lines_costed` is one to one at line grain: a line has one landed
    cost at the order date, and a fan-out here doubles the margin without
    changing the sales figure, which is exactly the shape nobody notices.
    """
    with warehouse.connect() as con:
        rows = con.execute(
            f"""SELECT s.category_id,
                       sum(s.net_sales_cents)::BIGINT                      AS net_sales_cents,
                       sum(c.landed_cost_cents)::BIGINT                    AS landed_cost_cents,
                       (sum(s.net_sales_cents) - sum(c.landed_cost_cents))::BIGINT
                                                                           AS merch_margin_cents
                FROM {warehouse.qualify('int.int_net_sales_lines')} s
                JOIN {warehouse.qualify('int.int_order_lines_costed')} c
                  ON c.order_line_id = s.order_line_id
                WHERE s.order_date = ?
                GROUP BY s.category_id
                ORDER BY s.category_id""",
            [target_ds],
        ).fetchall()
        return warehouse.delete_insert(
            table, "ds", target_ds,
            [(target_ds, *row) for row in rows],
            columns=["ds", "category_id", "net_sales_cents", "landed_cost_cents",
                     "merch_margin_cents"],
            con=con,
        )


def assert_channels(target_ds: str, table: str) -> list[str]:
    """Every channel landed a row for the day. Returns the channels found.

    A missing channel is a feed that did not arrive far more often than it is a
    day nobody bought anything, and the difference matters before the figure is
    published rather than after.
    """
    with warehouse.connect(read_only=True) as con:
        found = [row[0] for row in con.execute(
            f"SELECT DISTINCT channel FROM {warehouse.qualify(table)} WHERE ds = ?",
            [target_ds],
        ).fetchall()]
    missing = sorted(set(CHANNELS) - set(found))
    if missing:
        raise ValueError(f"{table}: no rows for {target_ds} in {', '.join(missing)}")
    return sorted(found)
