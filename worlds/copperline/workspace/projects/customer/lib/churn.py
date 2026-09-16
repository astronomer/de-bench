"""Churn features, the score, and what makes a score wrong.

Five inputs per account: how long since the last order, how often the account
orders, what an order is worth, how much support it takes, and whether it has
an open dispute. They are combined into one number between zero and one.

**Weights are here and nowhere else.** `WEIGHTS` is the model. It is a linear
score rather than anything cleverer because account managers read the parts
as well as the total, and a score they cannot explain is a score they ignore.

**The denominator is `int_customer_lifecycle`'s.** An account with no orders
in the window is not a churn risk; it is an account we never had. That
distinction is made once, in the shared model, for growth as well as for this
team.

**Both books, one score, and the era is on the row.** An acquired account is
scored on its own history, which starts when the book landed, so
`history_days` travels with the score and a manager can see what the number
is built on.
"""

from __future__ import annotations

import datetime as dt

from include.lib import warehouse

__all__ = ["WEIGHTS", "build_features", "build_scores", "score_problems",
           "score_counts", "publish"]

#: The score's parts and their weights. They sum to one, and the score is the
#: weighted sum of five values each already scaled to zero-to-one.
WEIGHTS = {
    "recency": 0.35,
    "frequency": 0.25,
    "value": 0.15,
    "support_load": 0.15,
    "open_dispute": 0.10,
}


def build_features(ds: str | dt.date) -> int:
    """The five inputs per account for the week. Returns the rows.

    Each one comes back scaled to zero-to-one, so the weights are the whole
    model and nothing is hidden in a unit.
    """
    day = _as_date(ds)
    lifecycle = warehouse.qualify("marts.customer_360")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT customer_id,
                   source_book,
                   least(1.0, coalesce(date_diff('day', last_order_date,
                                                 DATE '{day}'), 365) / 365.0)
                                                          AS recency,
                   1.0 - least(1.0, orders_12m / 24.0)     AS frequency,
                   1.0 - least(1.0, net_sales_cents / 5000000.0)
                                                          AS value,
                   least(1.0, coalesce(open_disputes, 0) / 3.0)
                                                          AS support_load,
                   CASE WHEN coalesce(open_disputes, 0) > 0 THEN 1.0
                        ELSE 0.0 END                       AS open_dispute,
                   coalesce(date_diff('day', first_order_date, DATE '{day}'), 0)
                                                          AS history_days,
                   DATE '{day}'                            AS ds
            FROM {lifecycle}
            WHERE status <> 'closed'
            ORDER BY customer_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "ops.churn_features", "ds", day, rows,
            columns=["customer_id", "source_book", *WEIGHTS, "history_days",
                     "ds"],
            con=con,
        )


def build_scores(ds: str | dt.date) -> int:
    """Combine the parts into one score per account. Returns the rows."""
    day = _as_date(ds)
    features = warehouse.qualify("ops.churn_features")
    weighted = " + ".join(f"{weight} * {part}" for part, weight in WEIGHTS.items())
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT customer_id, source_book,
                   round({weighted}, 4)  AS churn_score,
                   history_days,
                   DATE '{day}'          AS ds
            FROM {features} WHERE ds = DATE '{day}'
            ORDER BY customer_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "marts.churn_scores_weekly", "ds", day, rows,
            columns=["customer_id", "source_book", "churn_score",
                     "history_days", "ds"],
            con=con,
        )


def score_problems(ds: str | dt.date) -> list[dict]:
    """Scores outside zero-to-one, and accounts scored twice in a week."""
    day = _as_date(ds)
    table = warehouse.qualify("marts.churn_scores_weekly")
    with warehouse.connect(read_only=True) as con:
        out_of_range = con.execute(
            f"SELECT customer_id, churn_score FROM {table} "
            f"WHERE ds = DATE '{day}' AND (churn_score < 0 OR churn_score > 1) "
            "ORDER BY customer_id"
        ).fetchall()
        duplicates = con.execute(
            f"SELECT customer_id, count(*) FROM {table} WHERE ds = DATE '{day}' "
            "GROUP BY customer_id HAVING count(*) > 1 ORDER BY customer_id"
        ).fetchall()
    return ([{"problem": "score out of range", "customer_id": customer,
              "churn_score": float(score)} for customer, score in out_of_range]
            + [{"problem": "scored twice", "customer_id": customer,
                "rows": int(count)} for customer, count in duplicates])


def score_counts(ds: str | dt.date) -> dict[str, int]:
    """Accounts scored, and how many sit in the top decile."""
    day = _as_date(ds)
    table = warehouse.qualify("marts.churn_scores_weekly")
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"SELECT count(*), count(*) FILTER (churn_score >= 0.9), "
            f"count(*) FILTER (source_book = 'northwave') "
            f"FROM {table} WHERE ds = DATE '{day}'"
        ).fetchone()
    return {"accounts": int(row[0]), "top_decile": int(row[1]),
            "acquired_book": int(row[2])}


def publish(ds: str | dt.date) -> str:
    """Write the week's partition for the account managers' report."""
    day = _as_date(ds)
    table = warehouse.qualify("marts.churn_scores_weekly")
    with warehouse.connect(read_only=True) as con:
        result = con.execute(
            f"SELECT customer_id, source_book, churn_score, history_days "
            f"FROM {table} WHERE ds = DATE '{day}' ORDER BY churn_score DESC"
        )
        header = [description[0] for description in result.description]
        rows = result.fetchall()
    return str(warehouse.write_partition("marts", "churn_scores", day, header,
                                         rows))


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
