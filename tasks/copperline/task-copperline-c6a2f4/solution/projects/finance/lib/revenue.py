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

#: Where a line's money goes when the catalog does not carry its SKU. The order
#: book sells a wider SKU range than the catalog holds — `dim_product` says so
#: and it is about nine tenths of the day — so the join to the catalog is a left
#: join and the misses land here. Dropping them would leave the day's net sales
#: short of the shared model and `tie_to_sales` would fail.
UNCATALOGUED = "UNCATALOGUED"


def build_daily(target_ds: str, table: str) -> int:
    """Replace one day of `marts.daily_revenue`. Returns the rows written.

    One row per channel. `booked_cents` is the order's value on the order date,
    gross of nothing and net of nothing — `docs/semantic-definitions.md` fixes
    that meaning and it is not the recognised figure.

    The source is `int_orders_enriched`, the shared model commerce and growth
    also read. Finance does not rebuild the order spine.
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

    Three things this join gets wrong easily, and all three are quiet:

    * **`order_line_key` is the line, `order_line_id` is not.** The ordinal
      holds twelve values and repeats in every order — `int_order_lines_discounted`
      says so in as many words — so matching on it matches a line against every
      other line that happens to be the same ordinal. The costed model does not
      carry the ordinal at all, which is the shape of the answer: the key both
      models publish is the key.
    * **`extended_cost_cents` is the line's cost. `unit_cost_cents` is one
      unit's.** The costed model publishes both and extends the second by `qty`
      into the first, once, rounded half up to the cent. Summing the unit cost
      leaves net sales exactly right and understates the cost by the average
      quantity, so `tie_to_sales` passes, every other check passes, and the
      margin comes out several times too wide.
    * **The catalog misses most of the book.** `dim_product` carries a narrower
      SKU range than the order book sells, so an inner join to it drops about
      nine tenths of the day's money and `tie_to_sales` fails. The join is a
      left join and the misses land under `UNCATALOGUED`.
    """
    with warehouse.connect() as con:
        rows = con.execute(
            f"""SELECT coalesce(p.category_id, '{UNCATALOGUED}')      AS category_id,
                       sum(s.net_sales_cents)::BIGINT                 AS net_sales_cents,
                       sum(c.extended_cost_cents)::BIGINT             AS landed_cost_cents,
                       (sum(s.net_sales_cents) - sum(c.extended_cost_cents))::BIGINT
                                                                      AS merch_margin_cents
                FROM {warehouse.qualify('int.int_net_sales_lines')} s
                JOIN {warehouse.qualify('int.int_order_lines_costed')} c
                  ON c.order_line_key = s.order_line_key
                LEFT JOIN {warehouse.qualify('marts.dim_product')} p
                  ON p.sku = s.sku
                WHERE s.order_date = ?
                GROUP BY 1
                ORDER BY 1""",
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
