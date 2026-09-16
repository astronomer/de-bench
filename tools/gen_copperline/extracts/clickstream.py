"""S4 — `raw.web_events` and the Driftwood collector's hourly parquet.

The world's one big fixture, and the scale knob that makes the wall-clock cap
bind (spec 03 section 9).

## Where the scale knob lives

`volumes.clickstream_scale` in `worlds/copperline/timeline.yaml`. **The
shipped value is 0.25**, which is the profile spec 03 sections 9 and 20 both
quote, and it produces about 3,100 events a day. Raising it to 1.0
multiplies the feed by four and nothing else in the world moves: no other
table reads it, and every armed population here is stated at the shipped
scale and moves with it, so the boundary cases survive the change.
`SHIPPED_SCALE` below is the fallback when the key is absent, so an older
`timeline.yaml` still generates the shipped feed.

The `--profile small` divisor applies on top of the knob, through
`ctx.scale`, exactly as it does for every other fact volume.

## The numbers are the postmortem's numbers

`ops/incidents/2026-01-14-feed-decay.md` and
`ops/incidents/2026-01-23-event-replay.md` ship inside the world and print
exact counts. Spec 03 section 9 says outright that the postmortem's figures
are the fixture figures, so those two documents are the contract this module
meets:

  * a 3,100-event baseline with a standard deviation near 250 over the 90
    days to 2026-01-10, a lowest day of 2,510 and a highest of 3,760;
  * the decay days, 3,122 / 2,638 / 2,242 / 1,888 / 0;
  * a replay of 6,940 events, 1,102 of them already in the warehouse and
    5,838 new.

Two deliberate deviations from chapter 03 section 9 follow from that:

  * **No 30x Black Friday.** A 30x peak inside the baseline window would put
    the 90-day mean near 4,100 and its standard deviation near 9,700, and
    every number in the postmortem would be false. The peaks are the two
    days the postmortem calls "known peaks", at +21% and +19%.
  * **The replay is 6,940 rows, not 42,000.** 42,000 is four days of this
    feed at `--scale 1.0`, which is where the chapter's figure comes from.
    The shipped feed's replay is the one the incident note counts.

## The weekday shape is the clickstream's own

Orders swing 0.86 to 1.48 across the week. A web feed does not, and one that
did could not have a standard deviation of 8% of its mean. `WEEKDAY` below
is flat by comparison and averages exactly 1.0, so the baseline level is the
level and not an artefact of where the range starts.

## What it reads

`_util.days` for the day series and the day number, `sim_ext.ads_campaigns`
for the campaign a session came in on, `sim_product.product_variants` for
the SKUs, and — when the sales module has built — `sim_sales.orders` with
`sim_store.channels` for the orders a `purchase` event points at. Without
sales it falls back to the shared order-identity formula that
`upstream/logistics.py` documents, so the ids are ones `raw.orders` carries
either way.

The `ecommerce.*` tables are **not** the source. Spec 02 section 9 says the
web platform stitches its sessions from this collector's events, so the
event stream is the primitive and the sessions are derived from it.

## The delivery

`landing/events/dt=<ds>/hr=<hh>/producer=<p>/part-000.parquet`, partitioned
on the **load** clock, because a landing partition is a delivery and the
replay wrote its rows into partitions dated at the replay. RET-2 gives this
feed 400 live days, which is 400 x 24 x 5 = 48,000 parquet files; trial
setup cannot pay for that, so the tree holds `LANDING_DAYS` and `raw` holds
the history. `volumes.clickstream_landing_days` moves it.
"""

from __future__ import annotations

import datetime as dt

from ..config import Context
from ..streams import draw, pick, uniform
from ..upstream.logistics import ORDER_ID_BASE, SLOTS_PER_DAY
from . import _land

# --- the scale knob ---------------------------------------------------------

SHIPPED_SCALE = 0.25
BASELINE_EVENTS_PER_DAY = 3_100        # at SHIPPED_SCALE, per the postmortem
LANDING_DAYS = 7

# --- the day model ----------------------------------------------------------

# Sunday to Saturday, mean exactly 1.0. Flat next to the order shape.
WEEKDAY = (0.97, 1.03, 1.04, 1.03, 1.01, 0.96, 0.96)

# The growth curve is anchored at the middle of the postmortem's 90-day
# window, so "3,100 a day" is true where the postmortem measured it.
GROWTH_ANCHOR = dt.date(2025, 11, 26)

# Daily counts the shipped documents state outright, at SHIPPED_SCALE.
PINNED_DAYS = {
    dt.date(2025, 11, 11): 2_580,   # regional closure, Veterans Day
    dt.date(2025, 11, 27): 2_510,   # regional closure, Thanksgiving; the low
    dt.date(2025, 11, 28): 3_760,   # known peak, Black Friday; the high
    dt.date(2025, 12,  1): 3_690,   # known peak, Cyber Monday
    # Christmas and New Year fall inside the postmortem's 90-day window. It
    # counts four days outside the two-sigma band and names all four, so
    # these two are pinned well inside it rather than left to the draw.
    dt.date(2025, 12, 25): 2_760,
    dt.date(2026,  1,  1): 2_850,
    dt.date(2026,  1,  9): 3_100,   # the day the 10th is +0.7% against
    dt.date(2026,  1, 10): 3_122,
    dt.date(2026,  1, 11): 2_638,   # decay day one
    dt.date(2026,  1, 12): 2_242,   # decay day two
    dt.date(2026,  1, 13): 1_888,   # decay day three
    dt.date(2026,  1, 14): 0,       # hard failure
    dt.date(2026,  3, 21): 0,       # warehouse migration, quiet-days.yml
    dt.date(2026,  3, 22): 0,       # warehouse migration, quiet-days.yml
}

# The feeds pause at 18:00 on the Friday, so that day loses its evening.
PAUSE_FROM = (dt.date(2026, 3, 20), 18)

# Days the whole company is quiet and the website is not shut. Held inside
# the two-sigma band on purpose: the postmortem counts four days outside it
# and names all four, and these are not among them.
QUIET_FACTOR = {(12, 25): 0.88, (1, 1): 0.90}

# The peak factors this feed gives the dates timeline.yaml names as spikes.
# The order spike is 30x and 18x; a web funnel for a trade merchant is not.
PEAK_FACTOR = {"black_friday": 1.21, "cyber_monday": 1.19}

# --- the January 2026 incidents ---------------------------------------------

DECAY_WINDOW = (dt.date(2026, 1, 11), dt.date(2026, 1, 14))
REPLAY_WINDOW = (dt.date(2026, 1, 20), dt.date(2026, 1, 22))

# Events the collector release dropped and the replay recovered, at
# SHIPPED_SCALE. Each is that day's true volume less the volume that landed.
DROPPED_BY_DAY = {
    dt.date(2026, 1, 11): 368,
    dt.date(2026, 1, 12): 951,
    dt.date(2026, 1, 13): 1_336,
    dt.date(2026, 1, 14): 3_183,
}
REPLAY_DROPPED = sum(DROPPED_BY_DAY.values())       # 5,838, "genuinely new"
REPLAY_DUPLICATES = 1_102                           # "already present"
REPLAY_TOTAL = REPLAY_DROPPED + REPLAY_DUPLICATES   # 6,940

# The replay wrote fresh partitions, so its offsets come from their own band
# and no offset from the first delivery can collide with one.
REPLAY_OFFSET_BASE = 900_000_000

# --- the stream shape -------------------------------------------------------

PRODUCERS = (("web", 46), ("ios", 18), ("android", 16),
             ("email_click", 8), ("nightly_batch", 12))
NIGHTLY_PRODUCER = "nightly_batch"

# The contract's per-producer bounds, in minutes: four hours for the live
# producers, twenty-six for the nightly batch. Both stay inside the bound.
FAST_LAG_MINUTES = (3, 239)
NIGHTLY_LAG_MINUTES = (1_200, 1_559)

PARTITIONS = 12

EVENT_NAMES = (("page_view", 62), ("product_view", 22), ("add_to_cart", 9),
               ("checkout_start", 4), ("purchase", 3))
PRODUCT_EVENTS = ("product_view", "add_to_cart", "purchase")

# Visits per hour of the day, midnight first. Furniture, but it gives the
# hourly partitions the shape an hourly feed has.
HOUR_SHAPE = (12, 8, 6, 5, 6, 11, 22, 38, 55, 66, 72, 74,
              71, 70, 72, 74, 76, 78, 74, 66, 56, 44, 30, 19)

CUSTOMER_NULL_PCT = 62          # spec 03 section 9
UTM_TAGGED_PCT = 35             # sessions that carry a campaign
EVENTS_PER_SESSION = 4

DEVICES = (("desktop", 41), ("mobile", 47), ("tablet", 12))
COUNTRIES = (("US", 62), ("CA", 9), ("GB", 14), ("IE", 4), ("DE", 11))

# Per cent of a day's orders that are web. Used with the shared
# order-identity formula, which `upstream/logistics.py` documents and owns,
# where the sales module has not built.
WEB_ORDER_SHARE = 31


def scale_of(ctx: Context) -> float:
    """The clickstream scale knob. Nothing else in the world reads it."""
    return float(ctx.cfg["volumes"].get("clickstream_scale", SHIPPED_SCALE))


def _at_profile(ctx: Context, rows: int, scale: float) -> int:
    """A count stated at `SHIPPED_SCALE`, moved to this run's scale and
    profile. Zero stays zero: a day the feed sent nothing sends nothing at
    every scale."""
    if rows == 0:
        return 0
    return ctx.scale(max(1, round(rows * scale / SHIPPED_SCALE)))


def _landing_days(ctx: Context) -> int:
    return int(ctx.cfg["volumes"].get("clickstream_landing_days", LANDING_DAYS))


def _day_volumes(ctx: Context) -> None:
    """`_util.clickstream_days` — one row per date carrying the events that
    landed on it, the events the collector dropped, and the hour the feed
    stops."""
    scale = scale_of(ctx)
    baseline = _at_profile(ctx, BASELINE_EVENTS_PER_DAY, scale)
    shape = ", ".join(str(x) for x in WEEKDAY)

    pinned = " ".join(
        f"WHEN ds = DATE '{day}' THEN {_at_profile(ctx, n, scale)}"
        for day, n in sorted(PINNED_DAYS.items()))
    dropped = " ".join(
        f"WHEN ds = DATE '{day}' THEN {_at_profile(ctx, n, scale)}"
        for day, n in sorted(DROPPED_BY_DAY.items()))

    factors: dict[dt.date, float] = {}
    for spike in ctx.cfg["volumes"].get("spikes") or ():
        factor = PEAK_FACTOR.get(spike.get("name"))
        if factor:
            factors[dt.date.fromisoformat(str(spike["date"]))] = factor
    for year in range(ctx.start.year, ctx.end.year + 1):
        for (month, day), factor in QUIET_FACTOR.items():
            factors.setdefault(dt.date(year, month, day), factor)
    factor_case = " ".join(
        f"WHEN ds = DATE '{day}' THEN {value}"
        for day, value in sorted(factors.items()) if day not in PINNED_DAYS)

    h = draw(ctx.seed, "'clickstream_day'", "ds")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.clickstream_days AS
        SELECT
            ds,
            dn,
            (CASE {pinned} ELSE (
                {baseline}
                * ([{shape}])[dayofweek(ds) + 1]
                * pow(1 + {ctx.cfg['volumes']['annual_growth_pct']} / 100.0,
                      datediff('day', DATE '{GROWTH_ANCHOR}', ds) / 365.0)
                * (CASE {factor_case} ELSE 1 END)
                * ({uniform(h, 900, 1100)} / 1000.0)
            ) END)::BIGINT                                  AS events,
            (CASE {dropped} ELSE 0 END)::BIGINT             AS dropped,
            (CASE WHEN ds = DATE '{PAUSE_FROM[0]}' THEN {PAUSE_FROM[1]} ELSE 24 END)
                                                            AS open_until_hour
        FROM _util.days
    """)


def _pool(ctx: Context, table: str, relation: str, order_by: str) -> int:
    """A lookup table a draw indexes into by `i`, and its row count.

    Every pool is joined, never held as an array. A 25,000-element list
    indexed once per event costs six seconds at the small profile alone,
    because the list is rebuilt for every vector; the join costs nothing.
    """
    ctx.sql(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT (row_number() OVER (ORDER BY {order_by}) - 1)::BIGINT AS i, *
        FROM ({relation})
    """)
    return ctx.sql(f"SELECT count(*) FROM {table}").fetchone()[0]


def _order_pool(ctx: Context) -> None:
    """`_util.clickstream_orders` — the order ids a day's `purchase` events
    may point at, numbered inside the day.

    Sales owns `raw.orders`; where its simulation is present the pool is that
    day's web-channel orders, which is the truth. Where a day has none, and
    where sales has not built at all, the pool comes from the shared
    order-identity formula, so the ids are still ones `raw.orders` will
    carry. Every date in the range gets a pool, so the join that reads it
    can be an inner one and no event is lost to a quiet day.
    """
    present = ctx.sql(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'sim_sales' AND table_name = 'orders'"
    ).fetchone()[0]
    source = f"""
        SELECT d.ds, {ORDER_ID_BASE} + d.dn * {SLOTS_PER_DAY} + g.i AS order_id
        FROM _util.days d,
             LATERAL (SELECT unnest(generate_series(
                 1, greatest(1, (d.orders * {WEB_ORDER_SHARE}) // 100))) AS i) g
    """
    if present:
        source = f"""
            SELECT o.order_datetime::DATE AS ds, o.order_id
            FROM sim_sales.orders o
            JOIN sim_store.channels c ON c.channel_id = o.channel_id
            WHERE c.channel_type = 'web'
              AND o.order_datetime::DATE BETWEEN DATE '{ctx.start}' AND DATE '{ctx.end}'
            UNION ALL
            SELECT d.ds, {ORDER_ID_BASE} + d.dn * {SLOTS_PER_DAY} + 1
            FROM _util.days d
            WHERE d.ds NOT IN (SELECT DISTINCT order_datetime::DATE
                               FROM sim_sales.orders o2
                               JOIN sim_store.channels c2
                                 ON c2.channel_id = o2.channel_id
                               WHERE c2.channel_type = 'web')
        """
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.clickstream_orders AS
        SELECT s.ds,
               (row_number() OVER (PARTITION BY s.ds ORDER BY s.order_id) - 1)::BIGINT
                   AS i,
               s.order_id,
               -- The shipped spelling: lpad over the simulated key truncates,
               -- so the reference comes from the boundary's key map.
               ok.raw_order_id
        FROM ({source}) s
        LEFT JOIN _util.order_keys ok ON ok.order_id = s.order_id
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE _util.clickstream_order_counts AS
        SELECT ds, count(*)::BIGINT AS n FROM _util.clickstream_orders GROUP BY 1
    """)


def _pools(ctx: Context) -> dict[str, int]:
    """The SKU and campaign pools. Returns each one's size, because the draw
    that indexes a pool has to know how big it is."""
    return {
        "sku": _pool(ctx, "_util.clickstream_skus", """
            SELECT 'SKU-' || lpad((variant_id - 550000)::VARCHAR, 5, '0') AS sku
            FROM sim_product.product_variants""", "sku"),
        "paid": _pool(ctx, "_util.clickstream_paid", """
            SELECT campaign_id AS paid_campaign FROM sim_ext.ads_campaigns
            WHERE platform <> 'larkspur'""", "paid_campaign"),
        "email": _pool(ctx, "_util.clickstream_email", """
            SELECT campaign_id AS email_campaign FROM sim_ext.ads_campaigns
            WHERE platform = 'larkspur'""", "email_campaign"),
    }


def _draw_events(ctx: Context, sizes: dict[str, int], table: str,
                 count_column: str, id_offset: int, stream: str) -> None:
    """One event table: `count_column` rows a day, every field drawn from its
    own stream so that adding a field never moves an existing one."""
    s = ctx.seed
    key = f"(d.ds::VARCHAR || '#{stream}#' || g.i::VARCHAR)"
    h = {name: draw(s, f"'evt_{name}'", key)
         for name in ("producer", "name", "session", "customer", "sku",
                      "order", "utm", "device", "country", "minute", "hour")}

    ctx.sql(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * EXCLUDE (open_until_hour) FROM (
        SELECT
            d.ds,
            d.open_until_hour,
            'evt_' || strftime(d.ds, '%Y%m%d') || '-'
                   || lpad(({id_offset} + g.i)::VARCHAR, 7, '0')  AS event_id,
            {pick(h['producer'], [(f"'{p}'", w) for p, w in PRODUCERS])}
                                                                  AS producer,
            {pick(h['name'], [(f"'{n}'", w) for n, w in EVENT_NAMES])}
                                                                  AS event_name,
            ({h['session']} % 100000)                             AS session_ord,
            ({h['session']} % {PARTITIONS})::INTEGER              AS "partition",
            {pick(h['hour'], [(str(i), w) for i, w in enumerate(HOUR_SHAPE)])}
                                                                  AS event_hour,
            ({h['minute']} % 60)                                  AS event_minute,
            CASE WHEN ({h['customer']}) % 100 >= {CUSTOMER_NULL_PCT}
                 THEN 'C-' || lpad((400000 + ({h['customer']}) % 4000)::VARCHAR,
                                   6, '0') END                    AS customer_ref,
            CASE WHEN event_name IN ({', '.join(f"'{e}'" for e in PRODUCT_EVENTS)})
                 THEN sk.sku END                                  AS sku,
            CASE WHEN event_name = 'purchase'
                 THEN od.raw_order_id
                 END                                              AS order_id,
            CASE WHEN producer = 'email_click' THEN 'email'
                 WHEN ({h['utm']}) % 100 < {UTM_TAGGED_PCT}
                 THEN (['beacon', 'tessera', 'solstice'])[
                          1 + (({h['utm']}) >> 8) % 3] END        AS utm_source,
            CASE WHEN producer = 'email_click' THEN 'email'
                 WHEN ({h['utm']}) % 100 < {UTM_TAGGED_PCT}
                 THEN (['cpc', 'display', 'social'])[
                          1 + (({h['utm']}) >> 12) % 3] END       AS utm_medium,
            CASE WHEN producer = 'email_click' THEN em.email_campaign
                 WHEN ({h['utm']}) % 100 < {UTM_TAGGED_PCT} THEN pd.paid_campaign
                 END                                              AS utm_campaign,
            {pick(h['device'], [(f"'{x}'", w) for x, w in DEVICES])} AS device,
            {pick(h['country'], [(f"'{x}'", w) for x, w in COUNTRIES])}
                                                                  AS country_code
        FROM (SELECT d.*, oc.n AS order_count
              FROM _util.clickstream_days d
              JOIN _util.clickstream_order_counts oc ON oc.ds = d.ds) d,
             LATERAL (SELECT unnest(generate_series(1, d.{count_column})) AS i) g
        JOIN _util.clickstream_skus sk   ON sk.i = ({h['sku']}) % {sizes['sku']}
        JOIN _util.clickstream_paid pd   ON pd.i = ({h['utm']}) % {sizes['paid']}
        JOIN _util.clickstream_email em  ON em.i = (({h['utm']}) >> 16) % {sizes['email']}
        JOIN _util.clickstream_orders od ON od.ds = d.ds
                                        AND od.i = ({h['order']}) % d.order_count
        WHERE d.{count_column} > 0
        )
        -- The feeds pause mid-afternoon on the migration Friday, so that
        -- day's evening events do not exist. ops/calendar/quiet-days.yml.
        WHERE event_hour < open_until_hour
    """)


def _assemble(ctx: Context) -> None:
    """`raw.web_events` — the first delivery, then the replay on top of it.

    `event_time_utc` carries zero seconds, so a minute truncation downstream
    is a no-op. `load_time` is the armed column: four hours behind for the
    four live producers, twenty-six for the nightly batch, and nothing is
    ever loaded before it happened or after `today`.
    """
    s = ctx.seed
    fast_lo, fast_hi = FAST_LAG_MINUTES
    night_lo, night_hi = NIGHTLY_LAG_MINUTES
    h_lag = draw(s, "'evt_lag'", "event_id")
    h_rep = draw(s, "'evt_replay_at'", "event_id")
    h_pick = draw(s, "'evt_replay_pick'", "event_id")
    replay_start, replay_end = REPLAY_WINDOW
    replay_minutes = ((replay_end - replay_start).days + 1) * 1440

    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.web_event_rows AS
        WITH delivered AS (
            SELECT e.* EXCLUDE (event_hour, event_minute),
                   e.ds::TIMESTAMP + INTERVAL 1 HOUR * e.event_hour
                                   + INTERVAL 1 MINUTE * e.event_minute
                       AS event_time_utc,
                   false AS is_replay
            FROM _util.web_events_base e
        ), recovered AS (
            SELECT e.* EXCLUDE (event_hour, event_minute),
                   e.ds::TIMESTAMP + INTERVAL 1 HOUR * e.event_hour
                                   + INTERVAL 1 MINUTE * e.event_minute
                       AS event_time_utc,
                   true AS is_replay
            FROM _util.web_events_dropped e
        ), resent AS (
            SELECT * REPLACE (true AS is_replay) FROM delivered
            WHERE ds BETWEEN DATE '{DECAY_WINDOW[0]}' AND DATE '{DECAY_WINDOW[1]}'
            ORDER BY ({h_pick}) % 1000000000
            LIMIT {_at_profile(ctx, REPLAY_DUPLICATES, scale_of(ctx))}
        )
        SELECT * FROM delivered
        UNION ALL SELECT * FROM recovered
        UNION ALL SELECT * FROM resent
    """)

    ctx.sql(f"""
        CREATE OR REPLACE TABLE raw.web_events AS
        SELECT * EXCLUDE (session_ord, is_replay) FROM (
            SELECT
                e.event_id,
                e.producer,
                e."partition",
                (CASE WHEN e.is_replay THEN {REPLAY_OFFSET_BASE} ELSE 0 END
                 + row_number() OVER (PARTITION BY e.producer, e."partition",
                                                   e.is_replay
                                      ORDER BY e.event_time_utc, e.event_id)
                )::BIGINT                                       AS "offset",
                'sess_' || strftime(e.ds, '%Y%m%d') || '-'
                        || lpad((e.session_ord // {EVENTS_PER_SESSION})::VARCHAR,
                                6, '0')                         AS session_id,
                'anon_' || lpad((e.session_ord * 7 % 100000)::VARCHAR, 6, '0')
                                                                AS anonymous_id,
                e.customer_ref,
                e.event_name,
                e.event_time_utc,
                CASE WHEN e.is_replay
                     THEN DATE '{replay_start}'::TIMESTAMP
                          + INTERVAL 1 MINUTE * (({h_rep}) % {replay_minutes})
                     WHEN e.producer = '{NIGHTLY_PRODUCER}'
                     THEN e.event_time_utc + INTERVAL 1 MINUTE
                          * {uniform(h_lag, night_lo, night_hi)}
                     ELSE e.event_time_utc + INTERVAL 1 MINUTE
                          * {uniform(h_lag, fast_lo, fast_hi)}
                     END                                        AS load_time,
                CASE e.event_name
                     WHEN 'purchase' THEN '/checkout/confirm'
                     WHEN 'checkout_start' THEN '/checkout'
                     WHEN 'add_to_cart' THEN '/cart'
                     WHEN 'page_view' THEN '/c/' || lower(e.country_code)
                     ELSE '/p/' || lower(coalesce(e.sku, 'unknown')) END
                                                                AS page_path,
                e.sku,
                e.order_id,
                e.utm_source, e.utm_medium, e.utm_campaign,
                e.device, e.country_code,
                e.session_ord, e.is_replay
            FROM _util.web_event_rows e
        )
        -- The world is complete as of `today` and knows nothing later. The
        -- last evening of the range has not been delivered yet, and the
        -- nightly batch behind it has not run.
        WHERE load_time < DATE '{ctx.today}'
    """)


def _land_files(ctx: Context) -> None:
    """The hourly parquet, partitioned on the load clock."""
    start = _land.window_start(ctx, _landing_days(ctx))
    _land.write_hive(
        ctx,
        f"""
        SELECT load_time::DATE                                      AS dt,
               lpad(date_part('hour', load_time)::VARCHAR, 2, '0')  AS hr,
               producer,
               event_id, "partition", "offset", session_id, anonymous_id,
               customer_ref, event_name, event_time_utc, load_time,
               page_path, sku, order_id,
               utm_source, utm_medium, utm_campaign, device, country_code
        FROM raw.web_events
        WHERE load_time >= DATE '{start}'
        """,
        ctx.landing / "events", "PARQUET", ("dt", "hr", "producer"), "part-000")


def _check(ctx: Context) -> None:
    """What this module promises, asserted on its own output."""
    late, seconds = ctx.sql("""
        SELECT count(*) FILTER (WHERE load_time < event_time_utc),
               count(*) FILTER (WHERE date_part('second', event_time_utc) <> 0)
        FROM raw.web_events
    """).fetchone()
    assert not late, f"{late} event(s) loaded before they happened"
    assert not seconds, f"{seconds} event time(s) carry seconds"

    # The per-producer bound is what a task measures, so it holds on every
    # first-delivery row and not on average. Replayed rows carry the replay's
    # clock by design and are excluded by their offset band.
    for producer, bound in ((NIGHTLY_PRODUCER, 26), ("web", 4), ("ios", 4),
                            ("android", 4), ("email_click", 4)):
        over = ctx.sql(
            f"SELECT count(*) FROM raw.web_events WHERE producer = '{producer}' "
            f'AND "offset" < {REPLAY_OFFSET_BASE} '
            f"AND load_time > event_time_utc + INTERVAL 1 HOUR * {bound}"
        ).fetchone()[0]
        assert not over, f"{over} {producer} row(s) past the {bound}h bound"

    # There is no clickstream from the marketplace. Spec 03 sections 9 and 19
    # both say so, and it is easy to add by accident.
    declared = ", ".join(f"'{p}'" for p, _ in PRODUCERS)
    stray = ctx.sql("SELECT count(*) FROM raw.web_events "
                    f"WHERE producer NOT IN ({declared})").fetchone()[0]
    assert not stray, f"{stray} row(s) from a producer nobody declared"


def build(ctx: Context) -> None:
    from ._boundary import ensure_keys_or_stub

    ensure_keys_or_stub(ctx)  # purchase-event order refs come from _util.order_keys
    _day_volumes(ctx)
    _order_pool(ctx)
    sizes = _pools(ctx)
    _draw_events(ctx, sizes, "_util.web_events_base", "events", 0, "live")
    _draw_events(ctx, sizes, "_util.web_events_dropped", "dropped",
                 1_000_000, "drop")
    _assemble(ctx)
    _check(ctx)
    _land_files(ctx)
