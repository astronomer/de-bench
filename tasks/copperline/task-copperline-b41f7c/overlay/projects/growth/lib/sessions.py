"""Sessions, the funnel over them, and the touch-to-order join.

Three DAGs share this module — `gro_sessionize_daily`, `gro_funnel_daily` and
`gro_attribution_daily` — and one repair DAG rebuilds through it. It is here
rather than in each DAG because conversion rate is a company number: growth
and customer both quote it, so the numerator and the denominator are defined
once. `int_sessions_funnel` in the dbt project is the same definition on the
model side, and the two are meant to agree.

**An event is a position in the stream.** Driftwood delivers at least once, so
the same batch can reach us twice, and the producer, the partition and the
offset are what say which delivery a row belongs to. The first row at a
position wins and the rest are dropped before anything is counted.
`ops/incidents/2026-01-23-event-replay.md` is the last time this mattered.

**A day means the event's own day.** `event_time_utc` decides which day a
session belongs to, not `load_time`, which is when Driftwood got round to
sending it.
"""

from __future__ import annotations

import datetime as dt

from include.lib import warehouse

__all__ = ["SESSION_COLUMNS", "FUNNEL_STEPS", "days_in_window", "days_pending",
           "build_sessions", "stitch_orders", "replayed_events",
           "duplicate_sessions", "record_volume", "build_funnel",
           "attribute_touches"]

#: The published session grain: one row per session per day.
SESSION_COLUMNS = (
    "ds", "session_id", "anonymous_id", "customer_ref", "device",
    "country_code", "utm_source", "utm_medium", "utm_campaign", "entry_page",
    "session_start", "session_end", "events", "product_views", "add_to_carts",
    "checkouts", "orders", "order_id",
)

#: The funnel, in order. Each step counts the sessions that reached it, so the
#: steps are cumulative and a session counted at `add_to_cart` is counted at
#: `page_view` too.
FUNNEL_STEPS = ("page_view", "product_view", "add_to_cart", "checkout_start",
                "purchase")


def days_in_window(start: dt.datetime, end: dt.datetime) -> list[dt.date]:
    """The event days the deliveries in `[start, end)` touched.

    A delivery window is a window of `load_time`, and the events inside it can
    belong to several days — the nightly producer runs a day behind by
    design. This is the list of days a rebuild has to cover, and it is
    usually one day and occasionally three.
    """
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            "SELECT DISTINCT event_time_utc::DATE "
            f"FROM {warehouse.qualify('raw.web_events')} "
            "WHERE load_time >= ? AND load_time < ? ORDER BY 1",
            [start, end],
        ).fetchall()
    return [row[0] for row in rows]


def days_pending(lookback_days: int) -> list[str]:
    """The recent event days whose sessions no longer account for their events.

    The comparison is a count of distinct `event_id` on each side, so a day
    that gained a late event is pending and a day that gained a replayed copy
    of an event it already had is not.

    "Recent" is measured from the newest event the feed holds, not from a
    clock: a run replaying an old window has to see that window's days, and a
    run on a quiet Sunday must not decide that every day is pending because
    the wall clock moved on.
    """
    events = warehouse.qualify("raw.web_events")
    sessions = warehouse.qualify("marts.fct_web_sessions")
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"""
            WITH latest AS (SELECT max(event_time_utc) AS at FROM {events}),
            arrived AS (
                SELECT event_time_utc::DATE            AS ds,
                       count(DISTINCT event_id)        AS events
                FROM {events}, latest
                WHERE event_time_utc >= latest.at - INTERVAL {int(lookback_days)} DAY
                GROUP BY 1
            ), built AS (
                SELECT ds, sum(events) AS events FROM {sessions} GROUP BY 1
            )
            SELECT a.ds FROM arrived a
            LEFT JOIN built b ON b.ds = a.ds
            WHERE b.events IS NULL OR b.events <> a.events
            ORDER BY a.ds
            """
        ).fetchall()
    return [row[0].isoformat() for row in rows]


def build_sessions(ds: str | dt.date) -> int:
    """Rebuild `marts.fct_web_sessions` for one event day. Returns the rows.

    The whole day is rebuilt and replaces what was there, per
    `docs/late-data-policy.md` §LD-2. Late events for a day that has already
    been published are ordinary, and appending them would double the sessions
    they belong to.

    Repeat deliveries are dropped first, by stream position, so a batch the
    collector sent twice is counted once.
    """
    day = _as_date(ds)
    events = warehouse.qualify("raw.web_events")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            WITH delivered AS (
                SELECT *
                FROM {events}
                WHERE event_time_utc >= DATE '{day}'
                  AND event_time_utc < DATE '{day}' + INTERVAL 1 DAY
                QUALIFY row_number() OVER (
                    PARTITION BY producer, "partition", "offset"
                    ORDER BY load_time) = 1
            )
            SELECT DATE '{day}'                                   AS ds,
                   e.session_id,
                   any_value(e.anonymous_id)                      AS anonymous_id,
                   max(e.customer_ref)                            AS customer_ref,
                   any_value(e.device)                            AS device,
                   any_value(e.country_code)                      AS country_code,
                   max(e.utm_source)                              AS utm_source,
                   max(e.utm_medium)                              AS utm_medium,
                   max(e.utm_campaign)                            AS utm_campaign,
                   arg_min(e.page_path, e.event_time_utc)         AS entry_page,
                   min(e.event_time_utc)                          AS session_start,
                   max(e.event_time_utc)                          AS session_end,
                   count(*)                                       AS events,
                   count(*)
                       FILTER (e.event_name = 'product_view')     AS product_views,
                   count(*)
                       FILTER (e.event_name = 'add_to_cart')      AS add_to_carts,
                   count(*)
                       FILTER (e.event_name = 'checkout_start')   AS checkouts,
                   count(*)
                       FILTER (e.event_name = 'purchase')         AS orders,
                   max(e.order_id)                                AS order_id
            FROM delivered e
            GROUP BY e.session_id
            ORDER BY e.session_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            "marts.fct_web_sessions", "ds", day, rows,
            columns=list(SESSION_COLUMNS), con=con,
        )


def stitch_orders(ds: str | dt.date) -> int:
    """Resolve guest sessions against the order they placed. Returns the rows
    changed.

    A session that converted while signed out carries an order and no
    customer. The order carries both, so the session can be resolved after
    the fact. Scoped to one day's partition, and running it twice changes
    nothing the second time.
    """
    day = _as_date(ds)
    sessions = warehouse.qualify("marts.fct_web_sessions")
    orders = warehouse.qualify("raw.orders")
    with warehouse.connect() as con:
        con.execute(
            f"""
            UPDATE {sessions} s
               SET customer_ref = o.customer_ref
              FROM {orders} o
             WHERE o.order_id = s.order_id
               AND s.ds = DATE '{day}'
               AND s.customer_ref IS NULL
               AND o.customer_ref IS NOT NULL
            """
        )
        changed = con.execute(
            f"SELECT count(*) FROM {sessions} "
            f"WHERE ds = DATE '{day}' AND order_id IS NOT NULL "
            "AND customer_ref IS NOT NULL"
        ).fetchone()
    return int(changed[0] or 0)


def replayed_events(days: list[str]) -> int:
    """Events that exist at more than one stream offset on those days.

    One `event_id`, two `offset` values, identical payloads. The replay is
    additive by design, so this is a count and not a failure.
    """
    if not days:
        return 0
    events = warehouse.qualify("raw.web_events")
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT event_id FROM {events}
                WHERE event_time_utc::DATE IN ({_dates(days)})
                GROUP BY event_id HAVING count(DISTINCT "offset") > 1
            )
            """
        ).fetchone()
    return int(row[0] or 0)


def duplicate_sessions(days: list[str]) -> list[dict]:
    """Session ids that appear more than once on one day.

    The grain of `marts.fct_web_sessions` is one row per session per day, so
    this is empty on a healthy build. When it is not, the rows name what
    broke rather than making the caller go and look.
    """
    if not days:
        return []
    sessions = warehouse.qualify("marts.fct_web_sessions")
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT ds, session_id, count(*) FROM {sessions} "
            f"WHERE ds IN ({_dates(days)}) GROUP BY ds, session_id "
            "HAVING count(*) > 1 ORDER BY ds, session_id"
        ).fetchall()
    return [{"ds": str(ds), "session_id": session_id, "rows": int(count)}
            for ds, session_id, count in rows]


def record_volume(ds: str | dt.date) -> int:
    """Write one day's session and event counts to `ops.session_daily`.

    `config/alerts.yml` watches this table rather than the fact, because an
    alert wants a narrow table it can scan and the fact is wide.
    """
    day = _as_date(ds)
    sessions = warehouse.qualify("marts.fct_web_sessions")
    with warehouse.connect() as con:
        row = con.execute(
            f"SELECT count(*), coalesce(sum(events), 0), "
            f"count(*) FILTER (orders > 0) FROM {sessions} WHERE ds = DATE '{day}'"
        ).fetchone()
        return warehouse.delete_insert(
            "ops.session_daily", "ds", day,
            [{"ds": day, "sessions": int(row[0]), "events": int(row[1]),
              "converting_sessions": int(row[2])}],
            con=con,
        )


def build_funnel(ds: str | dt.date) -> int:
    """Rebuild `marts.funnel_daily` for one day. Returns the rows.

    One row per step per day, counting the sessions that reached the step.
    The denominator is every session that day, so the first step is the
    session count and the last is the converting sessions.
    """
    day = _as_date(ds)
    sessions = warehouse.qualify("marts.fct_web_sessions")
    reached = {
        "page_view": "true",
        "product_view": "product_views > 0",
        "add_to_cart": "add_to_carts > 0",
        "checkout_start": "checkouts > 0",
        "purchase": "orders > 0",
    }
    counts = ", ".join(
        f"count(*) FILTER ({reached[step]}) AS {step}" for step in FUNNEL_STEPS
    )
    with warehouse.connect() as con:
        row = con.execute(
            f"SELECT count(*) AS sessions, {counts} FROM {sessions} WHERE ds = DATE '{day}'"
        ).fetchone()
        total, per_step = row[0], row[1:]
        rows = [
            {"ds": day, "step": step, "position": position,
             "sessions": int(count), "sessions_total": int(total)}
            for position, (step, count) in enumerate(zip(FUNNEL_STEPS, per_step), 1)
        ]
        return warehouse.delete_insert("marts.funnel_daily", "ds", day, rows, con=con)


def attribute_touches(ds: str | dt.date, lookback_days: int) -> int:
    """Attribute a day's orders to the campaigns that touched them.

    Last non-direct touch inside the lookback window, which is the model the
    ad platforms report against and the only one their numbers can be
    compared to. A session with no campaign is a direct touch and is credited
    to `direct` rather than dropped, so the channels sum to the orders.

    Returns the rows written to `marts.agg_channel_funnel_daily`.
    """
    day = _as_date(ds)
    sessions = warehouse.qualify("marts.fct_web_sessions")
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            WITH converted AS (
                SELECT order_id, customer_ref, anonymous_id, session_start
                FROM {sessions}
                WHERE ds = DATE '{day}' AND orders > 0 AND order_id IS NOT NULL
            ), touches AS (
                SELECT c.order_id,
                       coalesce(s.utm_source, 'direct')            AS channel,
                       s.utm_campaign,
                       s.session_start,
                       row_number() OVER (PARTITION BY c.order_id
                                          ORDER BY s.utm_source IS NULL,
                                                   s.session_start DESC)  AS rank
                FROM converted c
                JOIN {sessions} s
                  ON s.anonymous_id = c.anonymous_id
                 AND s.session_start <= c.session_start
                 AND s.session_start > c.session_start
                                       - INTERVAL {int(lookback_days)} DAY
            )
            SELECT DATE '{day}'          AS ds,
                   channel,
                   coalesce(utm_campaign, 'none')  AS campaign,
                   count(*)              AS attributed_orders
            FROM touches
            WHERE rank = 1
            GROUP BY channel, campaign
            ORDER BY channel, campaign
            """
        ).fetchall()
        return warehouse.delete_insert(
            "marts.agg_channel_funnel_daily", "ds", day, rows,
            columns=["ds", "channel", "campaign", "attributed_orders"], con=con,
        )


def funnel_out_of_order(days: list[str]) -> list[dict]:
    """Funnel steps that count more sessions than the step before them.

    The steps are cumulative, so this is empty on a healthy build. When it is
    not, a join has fanned out and the conversion rate reads above one
    hundred per cent.
    """
    if not days:
        return []
    funnel = warehouse.qualify("marts.funnel_daily")
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT ds, step, sessions, previous FROM (
                SELECT ds, step, position, sessions,
                       lag(sessions) OVER (PARTITION BY ds ORDER BY position)
                           AS previous
                FROM {funnel} WHERE ds IN ({_dates(days)})
            ) WHERE previous IS NOT NULL AND sessions > previous
            ORDER BY ds, position
            """
        ).fetchall()
    return [{"ds": str(ds), "step": step, "sessions": int(count),
             "previous_step_sessions": int(previous)}
            for ds, step, count, previous in rows]


def attribution_gaps(days: list[str]) -> list[dict]:
    """Days where the attributed orders and the converting sessions disagree.

    Every converting session earns exactly one credit, so the two counts are
    equal on a healthy day. A gap is a session that converted against an
    order the fact does not carry, or a credit written twice.
    """
    if not days:
        return []
    sessions = warehouse.qualify("marts.fct_web_sessions")
    attributed = warehouse.qualify("marts.agg_channel_funnel_daily")
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"""
            WITH converted AS (
                SELECT ds, count(*) AS sessions FROM {sessions}
                WHERE ds IN ({_dates(days)}) AND orders > 0 GROUP BY ds
            ), credited AS (
                SELECT ds, sum(attributed_orders) AS orders FROM {attributed}
                WHERE ds IN ({_dates(days)}) GROUP BY ds
            )
            SELECT c.ds, c.sessions, coalesce(a.orders, 0)
            FROM converted c LEFT JOIN credited a ON a.ds = c.ds
            WHERE coalesce(a.orders, 0) <> c.sessions
            ORDER BY c.ds
            """
        ).fetchall()
    return [{"ds": str(ds), "converting_sessions": int(sessions),
             "attributed_orders": int(orders)}
            for ds, sessions, orders in rows]


def _dates(days: list[str]) -> str:
    """A day list as a SQL list of DATE literals."""
    return ", ".join(f"DATE '{_as_date(day)}'" for day in days)


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
