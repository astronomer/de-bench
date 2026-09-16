"""Churn features, the score, and what makes a score wrong.

Five inputs per account: how long since the last order, how often the account
orders, what an order is worth, how much support it takes, and whether it has
an open dispute. They are combined into one number between zero and one.

**Weights are here and nowhere else.** `WEIGHTS` is the model. It is a linear
score rather than anything cleverer because account managers read the parts
as well as the total, and a score they cannot explain is a score they ignore.

**Every part is held inside zero-to-one.** See `_bounded`. A part that leaves
its unit is not a smaller score, it is a weight nobody agreed to, and the
recency part leaves it on any week that is not the week the job runs on: an
account that ordered after the week being scored counts back to a negative
number of days.

**This table is `marts.customer_churn_weekly` and the scale is on the row.**
The nightly model writes a different churn score, out of a hundred, under the
name `marts.churn_scores_weekly`, and the two are not convertible. `TABLE`
names ours in one place and `SCALE` travels with every row, so a page in front
of an account manager says which of the two numbers it is holding.

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

__all__ = ["WEIGHTS", "TABLE", "SCALE", "build_features", "build_scores",
           "score_problems", "score_counts", "publish"]

#: The score's parts and their weights. They sum to one, and the score is the
#: weighted sum of five values each already scaled to zero-to-one.
WEIGHTS = {
    "recency": 0.35,
    "frequency": 0.25,
    "value": 0.15,
    "support_load": 0.15,
    "open_dispute": 0.10,
}

#: The features this job writes, and the scores it publishes. The score table
#: is this team's own: `marts.churn_scores_weekly` is the nightly model's name
#: and the nightly model keeps it.
FEATURES_TABLE = "ops.churn_features"
TABLE = "marts.customer_churn_weekly"

#: What the number on a row means. There are two churn scores in this
#: warehouse and neither name says which is which, so this one says it.
SCALE = "0-1"

#: The score table's columns, in the order they are written.
COLUMNS = ["customer_id", "source_book", "churn_score", "score_scale",
           "history_days", "ds"]

_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    customer_id     VARCHAR NOT NULL,
    source_book     VARCHAR,
    churn_score     DOUBLE  NOT NULL,
    score_scale     VARCHAR NOT NULL,
    history_days    BIGINT,
    ds              DATE    NOT NULL
)
"""


def _bounded(part: str) -> str:
    """One part of the score, held inside zero-to-one.

    Every part is a fraction of its own span and the weights assume that. A
    part that comes back outside the unit has stopped being a fraction: the
    recency part counts days from the last order to the week being scored, and
    an account that ordered after that week counts back a negative number of
    days, which subtracts recency's weight from the total instead of adding
    part of it. `check_scores` catches the result rather than the cause, so
    the bound belongs here.
    """
    return f"greatest(0.0, least(1.0, {part}))"


def build_features(ds: str | dt.date) -> int:
    """The five inputs per account for the week. Returns the rows.

    Each one comes back scaled to zero-to-one, so the weights are the whole
    model and nothing is hidden in a unit.
    """
    day = _as_date(ds)
    lifecycle = warehouse.qualify("marts.customer_360")
    recency = _bounded(
        f"coalesce(date_diff('day', last_order_date, DATE '{day}'), 365) / 365.0")
    frequency = _bounded("1.0 - orders_12m / 24.0")
    value = _bounded("1.0 - net_sales_cents / 5000000.0")
    support_load = _bounded("coalesce(open_disputes, 0) / 3.0")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT customer_id,
                   source_book,
                   {recency}                               AS recency,
                   {frequency}                             AS frequency,
                   {value}                                 AS value,
                   {support_load}                          AS support_load,
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
            FEATURES_TABLE, "ds", day, rows,
            columns=["customer_id", "source_book", *WEIGHTS, "history_days",
                     "ds"],
            con=con,
        )


def build_scores(ds: str | dt.date) -> int:
    """Combine the parts into one score per account. Returns the rows."""
    day = _as_date(ds)
    features = warehouse.qualify(FEATURES_TABLE)
    weighted = " + ".join(f"{weight} * {part}" for part, weight in WEIGHTS.items())
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('marts')}")
        con.execute(_DDL.format(table=warehouse.qualify(TABLE)))
        rows = con.execute(
            f"""
            SELECT customer_id, source_book,
                   round({weighted}, 4)  AS churn_score,
                   '{SCALE}'             AS score_scale,
                   history_days,
                   DATE '{day}'          AS ds
            FROM {features} WHERE ds = DATE '{day}'
            ORDER BY customer_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            TABLE, "ds", day, rows, columns=COLUMNS, con=con,
        )


def score_problems(ds: str | dt.date) -> list[dict]:
    """Scores outside zero-to-one, and accounts scored twice in a week."""
    day = _as_date(ds)
    table = warehouse.qualify(TABLE)
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
    table = warehouse.qualify(TABLE)
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"SELECT count(*), count(*) FILTER (churn_score >= 0.9), "
            f"count(*) FILTER (source_book = 'northwave') "
            f"FROM {table} WHERE ds = DATE '{day}'"
        ).fetchone()
    return {"accounts": int(row[0]), "top_decile": int(row[1]),
            "acquired_book": int(row[2])}


def publish(ds: str | dt.date) -> str:
    """Write the week's partition for the account managers' report.

    The file keeps the name it has always had. Retention's link points at it,
    and a list that lands under a new name is a list nobody opens.
    """
    day = _as_date(ds)
    table = warehouse.qualify(TABLE)
    with warehouse.connect(read_only=True) as con:
        result = con.execute(
            f"SELECT customer_id, source_book, churn_score, score_scale, "
            f"history_days FROM {table} WHERE ds = DATE '{day}' "
            "ORDER BY churn_score DESC"
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
