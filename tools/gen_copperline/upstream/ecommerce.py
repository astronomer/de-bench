"""`sim_ecommerce` — digital behaviour upstream of the order, spec chapter 02
section 9.

Five tables: sessions, page views, carts, cart lines and on-site searches.
Only the collector's event stream is extracted today, as `raw.web_events`,
and the extract collapses sessions, page views and searches into that one
stream at `--scale 0.25`. The volumes here are set so that the collapse lands
on the ~3,100 events a day the feed-decay incident measures. The base below
is a base, not an average: the weekday shape and the annual growth carry the
average day about a fifth above it, and 1,760 sessions a day at the base
comes out at roughly 12,400 events on the average day, a quarter of which is
3,100.

That makes the session-to-order rate far higher than a real shop's. The
clickstream is a sample and the order spine is not, so the two cannot both
be at true scale; the incident's baseline is the number a task grades, so it
is the one that wins. `carts.converted_order_id` names a real web order of
the same day, which keeps the funnel joinable end to end.

Runs after `sales`: converted carts read `sim_sales.orders`, and the customer
pick pool is the one the sales module already ranked.
"""

from __future__ import annotations

from ..config import Context
from ..streams import minute_time, pick, uniform
from . import _util
from ._util import h, money

SESSIONS_PER_DAY = 1760
CART_SHARE_PERMILLE = 500        # sessions that build a cart
CART_CONVERT_PERMILLE = 500      # carts that become an order
SEARCH_SHARE_PERMILLE = 330      # sessions that search at least once

DEVICES = [("'mobile'", 58), ("'desktop'", 33), ("'tablet'", 9)]
UTM_SOURCES = [("'organic'", 31), ("'email'", 18), ("'paid_search'", 21),
               ("'social'", 14), ("'direct'", 12), ("'affiliate'", 4)]
PAGE_TYPES = [("'pdp'", 41), ("'plp'", 24), ("'home'", 15), ("'search'", 9),
              ("'cart'", 7), ("'checkout'", 4)]
LANDING = [("'/'", 34), ("'/promo/spring-basics'", 9), ("'/c/outdoor-living'", 14),
           ("'/c/hardware'", 12), ("'/c/garden'", 11), ("'/deals'", 10),
           ("'/c/home'", 10)]
SEARCH_TERMS = [
    "slate deck cushion", "deck screws", "patio umbrella", "cordless drill",
    "garden hose 50ft", "led shop light", "paint roller", "hedge trimmer",
    "shelf bracket", "outdoor rug", "planter box", "door handle",
    "insulation roll", "cushion cover", "solar lantern", "tile spacer",
]


def build(ctx: Context) -> None:
    _util.ensure_days(ctx)
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_ecommerce")
    _sessions(ctx)
    _page_views(ctx)
    _carts(ctx)
    _searches(ctx)
    _storyline(ctx)


def _sessions(ctx: Context) -> None:
    campaigns = _util.dep(ctx, "sim_customer", "marketing_campaigns",
                          _util.standalone_campaigns())
    per_day = ctx.scale(SESSIONS_PER_DAY)
    spike = _util.spike_case(ctx, "'web'", "d.ds")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_ecommerce._day AS
        SELECT d.ds, d.day_index,
               greatest(1, round({per_day} * d.weekday_factor * d.growth_factor
                                 * {spike}))::INTEGER AS sessions_target
        FROM _util.days d
    """)
    busiest = ctx.sql("SELECT max(sessions_target) FROM sim_ecommerce._day").fetchone()[0]
    if busiest >= _util.DAY_BLOCK:
        raise ValueError(f"the busiest day wants {busiest} sessions and the "
                         f"per-day key block holds {_util.DAY_BLOCK}")

    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_ecommerce._campaign_day AS
        SELECT d.ds,
               row_number() OVER (PARTITION BY d.ds ORDER BY c.campaign_id) AS rank,
               c.campaign_id
        FROM _util.days d
        JOIN {campaigns} c ON c.start_date <= d.ds AND c.end_date >= d.ds
    """)
    ctx.sql("CREATE OR REPLACE TABLE sim_ecommerce._campaign_day_n AS "
            "SELECT ds, count(*)::BIGINT AS n FROM sim_ecommerce._campaign_day "
            "GROUP BY ds")

    def draw(stream: str, alias: str) -> str:
        return h(ctx, f"ecommerce.web_sessions.{stream}", f"{alias}.ds", f"{alias}.i")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ecommerce.web_sessions (
            session_id BIGINT PRIMARY KEY,
            customer_id BIGINT,
            channel_id INTEGER,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            device_type VARCHAR,
            os VARCHAR,
            browser VARCHAR,
            utm_source VARCHAR,
            campaign_id INTEGER,
            landing_url VARCHAR,
            ip_country_id INTEGER,
            is_bounce BOOLEAN
        )""")
    device = pick(draw("device", "g"), DEVICES)
    views = (f"CASE WHEN ({draw('views', 'g')}) % 1000 < 240 THEN 1 "
             f"ELSE {uniform(f'(({draw('views', 'g')}) >> 10)', 2, 7)} END")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_ecommerce._session AS
        WITH gen AS (
            SELECT d.ds, d.day_index,
                   unnest(generate_series(1, d.sessions_target)) AS i
            FROM sim_ecommerce._day d
        ), k AS (
            SELECT g.ds, g.i, g.day_index * {_util.DAY_BLOCK} + g.i AS seq,
                   {device} AS device_type,
                   ({views})::INTEGER AS n_views,
                   {minute_time(draw('time', 'g'), 'g.ds')} AS started_at
            FROM gen g
        ), n AS (
            -- The pool sizes become columns before the rank is drawn, so both
            -- picks below stay hash joins.
            SELECT k.*, coalesce(ce.n, 0) AS cust_n,
                   greatest(coalesce(cn.n, 1), 1) AS campaign_n
            FROM k
            LEFT JOIN sim_sales._cust_eligible ce ON ce.ds = k.ds
            LEFT JOIN sim_ecommerce._campaign_day_n cn ON cn.ds = k.ds
        )
        SELECT {_util.SESSION_LO} + n.seq AS session_id, n.seq, n.i, n.ds,
               n.started_at, n.n_views, n.device_type,
               CASE WHEN ({draw('customer', 'n')}) % 100 < 45 AND n.cust_n > 0
                    THEN c.customer_id END AS customer_id,
               cd.campaign_id
        FROM n
        LEFT JOIN sim_sales._cust c
               ON c.rank = 1 + ({draw('customer', 'n')}) % greatest(n.cust_n, 1)
        LEFT JOIN sim_ecommerce._campaign_day cd
               ON cd.ds = n.ds
              AND cd.rank = 1 + ({draw('campaign', 'n')}) % n.campaign_n
    """)
    ctx.sql(f"""
        INSERT INTO sim_ecommerce.web_sessions
        SELECT k.session_id, k.customer_id, {_util.CHANNEL_ID['web']},
               k.started_at,
               k.started_at + INTERVAL 1 SECOND
                   * (k.n_views * 90 + ({draw('dwell', 'k')}) % 240),
               k.device_type,
               CASE k.device_type WHEN 'mobile' THEN
                    (CASE WHEN ({draw('os', 'k')}) % 100 < 54 THEN 'iOS' ELSE 'Android' END)
                    WHEN 'tablet' THEN
                    (CASE WHEN ({draw('os', 'k')}) % 100 < 62 THEN 'iPadOS' ELSE 'Android' END)
                    ELSE (CASE WHEN ({draw('os', 'k')}) % 100 < 58 THEN 'Windows'
                               WHEN ({draw('os', 'k')}) % 100 < 88 THEN 'macOS'
                               ELSE 'Linux' END) END,
               CASE k.device_type WHEN 'mobile' THEN
                    (CASE WHEN ({draw('os', 'k')}) % 100 < 54 THEN 'Safari' ELSE 'Chrome' END)
                    ELSE (CASE WHEN ({draw('browser', 'k')}) % 100 < 61 THEN 'Chrome'
                               WHEN ({draw('browser', 'k')}) % 100 < 79 THEN 'Safari'
                               WHEN ({draw('browser', 'k')}) % 100 < 93 THEN 'Edge'
                               ELSE 'Firefox' END) END,
               {pick(draw('utm', 'k'), UTM_SOURCES)},
               k.campaign_id,
               {pick(draw('landing', 'k'), LANDING)},
               {pick(draw('country', 'k'), [('1', 62), ('2', 9), ('3', 12), ('4', 4), ('5', 9), ('7', 4)])},
               k.n_views = 1
        FROM sim_ecommerce._session k
    """)


def _page_views(ctx: Context) -> None:
    variants = _util.dep(ctx, "sim_product", "product_variants",
                         _util.standalone_variants())
    products = _util.dep(ctx, "sim_product", "products", _util.standalone_products())
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_ecommerce._variant AS
        SELECT row_number() OVER (ORDER BY v.variant_id) AS rank, v.variant_id,
               coalesce(p.category_id, 300) AS category_id
        FROM {variants} v LEFT JOIN {products} p ON p.product_id = v.product_id
        WHERE v.status = 'active'
    """)
    n_variants = ctx.sql("SELECT count(*) FROM sim_ecommerce._variant").fetchone()[0]
    if not n_variants:
        raise ValueError("no active product variants to browse")

    def draw(stream: str, alias: str) -> str:
        return h(ctx, f"ecommerce.page_views.{stream}",
                 f"{alias}.ds", f"{alias}.seq", f"{alias}.k")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ecommerce.page_views (
            page_view_id BIGINT,
            session_id BIGINT,
            page_url VARCHAR,
            page_type VARCHAR,
            variant_id BIGINT,
            category_id INTEGER,
            viewed_at TIMESTAMP,
            dwell_seconds INTEGER
        )""")
    ctx.sql(f"""
        INSERT INTO sim_ecommerce.page_views
        WITH gen AS (
            SELECT s.session_id, s.ds, s.seq, s.started_at, s.n_views,
                   unnest(generate_series(1, s.n_views)) AS k
            FROM sim_ecommerce._session s
        ), t AS (
            SELECT g.*, {pick(draw('type', 'g'), PAGE_TYPES)} AS page_type
            FROM gen g
        ), v AS (
            SELECT t.*, vv.variant_id, vv.category_id
            FROM t LEFT JOIN sim_ecommerce._variant vv
                     ON vv.rank = 1 + ({draw('variant', 't')}) % {n_variants}
        )
        SELECT {_util.PAGE_VIEW_LO} + v.seq * 16 + v.k, v.session_id,
               CASE v.page_type
                    WHEN 'pdp' THEN '/p/' || v.variant_id::VARCHAR
                    WHEN 'plp' THEN '/c/' || v.category_id::VARCHAR
                    WHEN 'home' THEN '/'
                    WHEN 'search' THEN '/search'
                    WHEN 'cart' THEN '/cart'
                    ELSE '/checkout' END,
               v.page_type,
               CASE WHEN v.page_type = 'pdp' THEN v.variant_id END,
               CASE WHEN v.page_type IN ('pdp', 'plp') THEN v.category_id END,
               v.started_at + INTERVAL 1 SECOND * ((v.k - 1) * 90
                   + ({draw('offset', 'v')}) % 80),
               ({draw('dwell', 'v')}) % 240 + 8
        FROM v
    """)


def _carts(ctx: Context) -> None:
    """A cart belongs to a session; a converted cart names a web order of the
    same day. Orders outnumber carts every day, so the assignment is total."""
    def draw(stream: str, alias: str) -> str:
        return h(ctx, f"ecommerce.carts.{stream}", f"{alias}.ds", f"{alias}.seq")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ecommerce.carts (
            cart_id BIGINT PRIMARY KEY,
            session_id BIGINT,
            customer_id BIGINT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            cart_status VARCHAR,
            converted_order_id BIGINT,
            currency_code VARCHAR
        )""")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_ecommerce._cart AS
        WITH c AS (
            SELECT s.*, ({draw('convert', 's')}) % 1000 < {CART_CONVERT_PERMILLE}
                       AS converted
            FROM sim_ecommerce._session s
            WHERE ({draw('taken', 's')}) % 1000 < {CART_SHARE_PERMILLE}
              AND s.n_views > 1
        ), r AS (
            SELECT c.*, CASE WHEN c.converted THEN
                       row_number() OVER (PARTITION BY c.ds, c.converted
                                          ORDER BY c.session_id) END AS conv_rank
            FROM c
        ), o AS (
            SELECT x.order_id, b.ds,
                   row_number() OVER (PARTITION BY b.ds ORDER BY x.order_id) AS rank
            FROM sim_sales.orders x
            JOIN sim_sales._orders_base b ON b.order_id = x.order_id
            WHERE b.channel_type = 'web'
        )
        SELECT r.session_id, r.customer_id, r.ds, r.seq, r.started_at, r.n_views,
               r.converted, o.order_id AS converted_order_id
        FROM r LEFT JOIN o ON o.ds = r.ds AND o.rank = r.conv_rank
    """)
    ctx.sql(f"""
        INSERT INTO sim_ecommerce.carts
        SELECT {_util.CART_LO} + c.seq, c.session_id, c.customer_id,
               c.started_at + INTERVAL 1 SECOND * (60 + ({draw('created', 'c')}) % 600),
               c.started_at + INTERVAL 1 SECOND * (240 + ({draw('updated', 'c')}) % 1200),
               CASE WHEN c.converted AND c.converted_order_id IS NOT NULL
                    THEN 'converted'
                    WHEN ({draw('status', 'c')}) % 100 < 8 THEN 'active'
                    ELSE 'abandoned' END,
               CASE WHEN c.converted THEN c.converted_order_id END,
               'USD'
        FROM sim_ecommerce._cart c
    """)

    def line_draw(stream: str, alias: str) -> str:
        return h(ctx, f"ecommerce.cart_lines.{stream}",
                 f"{alias}.ds", f"{alias}.seq", f"{alias}.k")

    n_variants = ctx.sql("SELECT count(*) FROM sim_ecommerce._variant").fetchone()[0]
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ecommerce.cart_lines (
            cart_line_id BIGINT PRIMARY KEY,
            cart_id BIGINT,
            variant_id BIGINT,
            qty DECIMAL(12,3),
            unit_price DECIMAL(18,4),
            added_at TIMESTAMP,
            removed_at TIMESTAMP
        )""")
    ctx.sql(f"""
        INSERT INTO sim_ecommerce.cart_lines
        WITH gen AS (
            SELECT c.*, unnest(generate_series(1, 1 + ({h(ctx, 'ecommerce.cart_lines.count', 'c.ds', 'c.seq')}) % 4)) AS k
            FROM sim_ecommerce._cart c
        ), v AS (
            SELECT g.*, vv.variant_id
            FROM gen g JOIN sim_ecommerce._variant vv
                    ON vv.rank = 1 + ({line_draw('variant', 'g')}) % {n_variants}
        )
        SELECT {_util.CART_LINE_LO} + v.seq * 4 + v.k, {_util.CART_LO} + v.seq,
               v.variant_id,
               (1 + ({line_draw('qty', 'v')}) % 3)::DECIMAL(12,3),
               {money(f"4.99 + (hash({ctx.seed}, 'variant.price', v.variant_id) >> 1)::BIGINT % 24001 / 100.0")},
               v.started_at + INTERVAL 1 SECOND * (60 + ({line_draw('added', 'v')}) % 600),
               CASE WHEN ({line_draw('removed', 'v')}) % 100 < 14
                    THEN v.started_at + INTERVAL 1 SECOND
                         * (700 + ({line_draw('removed', 'v')}) % 900) END
        FROM v
    """)


def _searches(ctx: Context) -> None:
    def draw(stream: str, alias: str) -> str:
        return h(ctx, f"ecommerce.search_queries.{stream}",
                 f"{alias}.ds", f"{alias}.seq", f"{alias}.k")

    n_variants = ctx.sql("SELECT count(*) FROM sim_ecommerce._variant").fetchone()[0]
    terms = ", ".join(f"({i}, '{t}')" for i, t in enumerate(SEARCH_TERMS, start=1))
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_ecommerce.search_queries (
            search_id BIGINT PRIMARY KEY,
            session_id BIGINT,
            query_text VARCHAR,
            results_count INTEGER,
            clicked_variant_id BIGINT,
            click_position INTEGER,
            searched_at TIMESTAMP
        )""")
    ctx.sql(f"""
        INSERT INTO sim_ecommerce.search_queries
        WITH t(rank, term) AS (VALUES {terms}),
        gen AS (
            SELECT s.*, unnest(generate_series(1, 1 + ({h(ctx, 'ecommerce.search_queries.count', 's.ds', 's.seq')}) % 2)) AS k
            FROM sim_ecommerce._session s
            WHERE ({h(ctx, 'ecommerce.search_queries.taken', 's.ds', 's.seq')}) % 1000
                  < {SEARCH_SHARE_PERMILLE}
        ), v AS (
            SELECT g.*, vv.variant_id, t.term
            FROM gen g
            JOIN sim_ecommerce._variant vv
              ON vv.rank = 1 + ({draw('variant', 'g')}) % {n_variants}
            JOIN t ON t.rank = 1 + ({draw('term', 'g')}) % {len(SEARCH_TERMS)}
        )
        SELECT {_util.SEARCH_LO} + v.seq * 2 + v.k, {_util.SESSION_LO} + v.seq,
               v.term,
               CASE WHEN ({draw('results', 'v')}) % 100 < 6 THEN 0
                    ELSE ({draw('results', 'v')}) % 220 + 1 END,
               CASE WHEN ({draw('click', 'v')}) % 100 < 43 THEN v.variant_id END,
               CASE WHEN ({draw('click', 'v')}) % 100 < 43
                    THEN 1 + ({draw('click', 'v')}) % 10 END,
               v.started_at + INTERVAL 1 SECOND * (30 + ({draw('at', 'v')}) % 500)
        FROM v
    """)


def _storyline(ctx: Context) -> None:
    """Priya's visit, chapter 02 section 9: the session that searched for the
    cushion, viewed it, put three in a cart and checked out as order 8840127."""
    ctx.sql("""
        INSERT INTO sim_ecommerce.web_sessions VALUES
        (77120449, 90412, 2, TIMESTAMP '2026-04-14 19:44:00',
         TIMESTAMP '2026-04-14 20:02:00', 'mobile', 'iOS', 'Safari', 'email',
         1180, '/promo/spring-basics', 1, false)
    """)
    ctx.sql("""
        INSERT INTO sim_ecommerce.page_views VALUES
        (5501339287, 77120449, '/p/aurora-deck-cushion-slate', 'pdp', 550231,
         318, TIMESTAMP '2026-04-14 19:47:00', 63)
    """)
    ctx.sql("""
        INSERT INTO sim_ecommerce.carts VALUES
        (3320117, 77120449, 90412, TIMESTAMP '2026-04-14 19:49:00',
         TIMESTAMP '2026-04-14 19:58:00', 'converted', 8840127, 'USD')
    """)
    ctx.sql("""
        INSERT INTO sim_ecommerce.cart_lines VALUES
        (8810442, 3320117, 550231, 3.000, 29.9900,
         TIMESTAMP '2026-04-14 19:49:00', NULL)
    """)
    ctx.sql("""
        INSERT INTO sim_ecommerce.search_queries VALUES
        (4410228, 77120449, 'slate deck cushion', 47, 550231, 2,
         TIMESTAMP '2026-04-14 19:46:00')
    """)
