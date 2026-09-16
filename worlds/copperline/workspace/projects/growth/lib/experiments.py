"""Experiment exposures, outcomes, and whether the split held.

An experiment is a row in `raw.experiments` with a start date, an end date
and an agreed split across its arms. Assignment happens in the web shop and
reaches us on the session: `marts.fct_web_sessions` carries the experiment
and the arm a session was in.

Two numbers per arm per day, and they come from two places on purpose.
Exposure is a session count, from the session fact. Outcome is orders and
net sales, from the order spine, because `net_sales_cents` is defined once in
`docs/semantic-definitions.md` and growth does not get to define it a second
time.

Nothing here decides anything. There is no stopping rule, no significance
test and no winner: the readout publishes the arms and whoever reads it
applies a rule.
"""

from __future__ import annotations

import datetime as dt

from include.lib import warehouse

__all__ = ["SPLIT_TOLERANCE_BPS", "running_on", "build_exposures",
           "build_outcomes", "skewed_arms", "publish"]

#: How far an arm's share may sit from its agreed share before the split is
#: called broken, in basis points. Five hundred is five percentage points,
#: which is wider than daily noise on the smallest experiment we run.
SPLIT_TOLERANCE_BPS = 500


def running_on(ds: str | dt.date) -> list[dict]:
    """The experiments live on a day, with their arms and agreed shares.

    An experiment that ended that day is included: it was running then, and a
    readout of that day has to say so.
    """
    day = _as_date(ds)
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT experiment_id, arm, share_bps, started_on, ended_on
            FROM {warehouse.qualify('raw.experiment_arms')}
            WHERE started_on <= DATE '{day}'
              AND (ended_on IS NULL OR ended_on >= DATE '{day}')
            ORDER BY experiment_id, arm
            """
        ).fetchall()
    return [{"experiment_id": experiment, "arm": arm, "share_bps": int(share),
             "started_on": str(started),
             "ended_on": str(ended) if ended else None}
            for experiment, arm, share, started, ended in rows]


def build_exposures(ds: str | dt.date) -> int:
    """Sessions per experiment arm for one day. Returns the rows written."""
    day = _as_date(ds)
    sessions = warehouse.qualify("marts.fct_web_sessions")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT experiment_id, arm,
                   count(*)                          AS sessions,
                   count(*) FILTER (orders > 0)      AS converting_sessions
            FROM {sessions}
            WHERE ds = DATE '{day}' AND experiment_id IS NOT NULL
            GROUP BY experiment_id, arm ORDER BY experiment_id, arm
            """
        ).fetchall()
        return warehouse.delete_insert(
            "ops.experiment_exposure", "ds", day,
            [(experiment, arm, int(sessions), int(converting), day)
             for experiment, arm, sessions, converting in rows],
            columns=["experiment_id", "arm", "sessions",
                     "converting_sessions", "ds"],
            con=con,
        )


def build_outcomes(ds: str | dt.date) -> int:
    """Orders and net sales per arm for one day. Returns the rows written.

    The money comes from the order economics mart, which is where
    `net_sales_cents` is computed. Growth reads that definition; it does not
    write a second one.
    """
    day = _as_date(ds)
    sessions = warehouse.qualify("marts.fct_web_sessions")
    economics = warehouse.qualify("marts.order_economics")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT s.experiment_id, s.arm,
                   count(DISTINCT s.order_id)             AS orders,
                   coalesce(sum(e.net_sales_cents), 0)    AS net_sales_cents
            FROM {sessions} s
            LEFT JOIN {economics} e ON e.order_id = s.order_id
            WHERE s.ds = DATE '{day}' AND s.experiment_id IS NOT NULL
              AND s.order_id IS NOT NULL
            GROUP BY s.experiment_id, s.arm ORDER BY s.experiment_id, s.arm
            """
        ).fetchall()
        return warehouse.delete_insert(
            "ops.experiment_outcome", "ds", day,
            [(experiment, arm, int(orders), int(sales), day)
             for experiment, arm, orders, sales in rows],
            columns=["experiment_id", "arm", "orders", "net_sales_cents", "ds"],
            con=con,
        )


def skewed_arms(ds: str | dt.date, live: list[dict]) -> list[dict]:
    """Arms whose share of the day's exposure is outside the agreed split.

    A drifted split means the assignment is broken, and the readout is then a
    comparison of two different populations rather than of two treatments.
    """
    day = _as_date(ds)
    agreed = {(row["experiment_id"], row["arm"]): row["share_bps"]
              for row in live}
    if not agreed:
        return []
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT experiment_id, arm, sessions,
                   sum(sessions) OVER (PARTITION BY experiment_id) AS total
            FROM {warehouse.qualify('ops.experiment_exposure')}
            WHERE ds = DATE '{day}'
            """
        ).fetchall()
    skewed = []
    for experiment, arm, sessions, total in rows:
        expected = agreed.get((experiment, arm))
        if expected is None or not total:
            continue
        actual = round(10_000 * sessions / total)
        if abs(actual - expected) > SPLIT_TOLERANCE_BPS:
            skewed.append({"experiment_id": experiment, "arm": arm,
                           "expected_bps": expected, "actual_bps": actual})
    return skewed


def publish(ds: str | dt.date) -> str:
    """Write the day's readout partition and return the path."""
    day = _as_date(ds)
    readout = warehouse.qualify("marts.experiment_readout")
    with warehouse.connect(read_only=True) as con:
        result = con.execute(
            f"SELECT * FROM {readout} WHERE ds = DATE '{day}' "
            "ORDER BY experiment_id, arm"
        )
        header = [description[0] for description in result.description]
        rows = result.fetchall()
    return str(warehouse.write_partition("marts", "experiment_readout", day,
                                         header, rows))


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
