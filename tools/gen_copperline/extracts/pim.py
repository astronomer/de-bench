"""S9 — Palette's change feed: `raw.pim_product_versions`,
`raw.product_categories`, and `landing/pim/dt=<ds>/changes.csv`.

Not a snapshot. Only what changed, and not always in the order it changed.
That is the whole source: spec 03 section 14 arms four columns on this table
and every one of them is about the difference between when a change happened
and when the warehouse heard about it.

## The two clocks

`updated_at` is the source clock and orders the truth. `received_at` is the
load clock and does not. `docs/late-data-policy.md` says so outright, and
says the feed has no bound at all. Ordinary rows land in the next morning's
file; the armed ones do not.

## The armed populations, and how each one is built

Every SKU's role is fixed by its ordinal, so nothing is sampled and no count
can drift. None of them scales with the profile: they are the task, and 21,000
rows cost nothing to build at any size.

  ordinal 1          the out-of-order arrival that lands inside a **closed**
                     span — version 5 arrives after versions 6 and 7, so two
                     versions already sit either side of it.
  ordinal 2..140     the other 139 out-of-order arrivals. Version 5 arrives
                     after version 6, inside the span that was open when it
                     landed. **140 in total.**
  ordinal 141..178   38 same-day doubles from two different sources, which is
                     where the priority order has to decide.
  ordinal 179..236   58 same-day doubles from one source. **96 in total.**
  ordinal 237..258   22 resurrections: version 8 is a `delete` and version 9
                     brings the key back.
  ordinal 259..400   142 ordinary discontinuations: the last version is a
                     `delete` and nothing follows it. They exist so that
                     `operation = 'delete'` does not mean "resurrection".
  ordinal 401..1860  ordinary.

A change's date is placed inside its own slot of the range, so two versions
of one SKU never share a day by accident. Without that, eleven versions over
862 days collide about six times in a hundred, and 1,600 ordinary SKUs would
bury the 96 that are the point.

## The tie-break the contract still has to state

Where two versions of one SKU carry the same `updated_at` date, the source
decides, and the order is **`pim_ui` > `supplier_feed` > `bulk_load`**: a
person editing the record beats a supplier's file, and a bulk load is the
weakest because it is a re-import of what was already there. The fixture is
built on that order. No shipped document states it yet, and the SCD contract
a task is written against has to.

## Deviations from spec 03 section 14

  * The chapter names the table `raw.pim_product_versions` in section 14 and
    again in section 21. It is that. Some notes call it `raw.pim_changes`.
  * The chapter wants 420 categories. `sim_product.categories` builds 181, a
    four-level tree, and an extract does not invent rows its upstream does
    not hold. Widening the tree belongs to the catalog module.
"""

from __future__ import annotations

from ..config import Context
from ..streams import draw, pick
from . import _land

SKUS = 1_860
CHANGES = 21_000
# 540 SKUs carry twelve versions and the rest eleven, which is 21,000 exactly.
LONG_HISTORY_SKUS = 540
LONG_HISTORY = 12
SHORT_HISTORY = 11

# Roles by ordinal. Each boundary is a count from spec 03 section 14.
SPLIT_SPAN = 1                     # the one that lands inside a closed span
OUT_OF_ORDER = 140
DOUBLE_CROSS_SOURCE = 38
DOUBLE_SAME_SOURCE = 58
DOUBLES = DOUBLE_CROSS_SOURCE + DOUBLE_SAME_SOURCE      # 96
RESURRECTIONS = 22
DISCONTINUATIONS = 142

_OUT_OF_ORDER_END = OUT_OF_ORDER                        # 140
_DOUBLE_CROSS_END = _OUT_OF_ORDER_END + DOUBLE_CROSS_SOURCE     # 178
_DOUBLE_END = _OUT_OF_ORDER_END + DOUBLES                       # 236
_RESURRECTION_END = _DOUBLE_END + RESURRECTIONS                 # 258
_DISCONTINUED_END = _RESURRECTION_END + DISCONTINUATIONS        # 400

# Which version of a SKU's history each armed population lands on.
LATE_VERSION = 5          # the version that arrives out of order
DOUBLE_VERSION = 7        # the version dragged onto the previous one's day
DELETE_VERSION = 8        # the delete a later version resurrects from

SOURCES = (("pim_ui", 55), ("supplier_feed", 30), ("bulk_load", 15))
# The order the same-day tie-break follows. Strongest first.
SOURCE_PRIORITY = ("pim_ui", "supplier_feed", "bulk_load")

FEED_HOUR = 2             # the change feed lands at 02:xx the next morning


def _skus(ctx: Context) -> None:
    """`_util.pim_skus` — the 1,860 SKUs Palette manages, each with the
    ordinal that fixes its role and the catalog facts its versions carry."""
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.pim_skus AS
        SELECT
            row_number() OVER (ORDER BY v.variant_id)              AS n,
            'SKU-' || lpad((v.variant_id - 550000)::VARCHAR, 5, '0') AS sku,
            p.name                                                 AS product_name,
            p.category_id,
            b.name                                                 AS brand,
            'SUP-' || lpad(b.supplier_id::VARCHAR, 4, '0')         AS supplier_id,
            v.color, v.size,
            CASE WHEN row_number() OVER (ORDER BY v.variant_id) <= {LONG_HISTORY_SKUS}
                 THEN {LONG_HISTORY} ELSE {SHORT_HISTORY} END      AS versions
        FROM sim_product.product_variants v
        JOIN sim_product.products p ON p.product_id = v.product_id
        JOIN sim_product.brands b ON b.brand_id = p.brand_id
        WHERE v.variant_id <= 550000 + {SKUS}
    """)
    n = ctx.sql("SELECT count(*) FROM _util.pim_skus").fetchone()[0]
    assert n == SKUS, f"the PIM covers {n} SKUs, not {SKUS}"


def _versions(ctx: Context) -> None:
    """`_util.pim_versions` — every change, on its own day, in order.

    A SKU's `versions` changes divide the range into that many slots, and
    version `k` lands anywhere in slot `k`. The jitter is one day short of
    the slot, so the slots cover the range with no comb of empty weeks in
    it, and the versions of one SKU are still strictly increasing and never
    share a date unless this module puts them there.
    """
    s = ctx.seed
    span = (ctx.end - ctx.start).days - 1
    key = ("k.sku", "v.k")
    h_day = draw(s, "'pim_day'", *key)
    h_min = draw(s, "'pim_minute'", *key)
    h_price = draw(s, "'pim_price'", *key)
    h_source = draw(s, "'pim_source'", *key)
    h_name = draw(s, "'pim_rename'", *key)
    h_status = draw(s, "'pim_status'", *key)

    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.pim_versions AS
        SELECT
            k.n, k.sku, v.k, k.versions,
            ({span} * v.k) // k.versions
                + ({h_day}) % ({span} // k.versions)                  AS day_no,
            ({h_min} % 1440)                                          AS minute_no,
            k.category_id, k.brand, k.supplier_id, k.color, k.size,
            CASE WHEN v.k > 0 AND ({h_name}) % 100 < 12
                 THEN k.product_name || ' ' || k.color
                 ELSE k.product_name END                              AS product_name,
            (2000 + ({h_price}) % 240000)                             AS list_price_cents,
            CASE WHEN v.k = k.versions - 1 AND ({h_status}) % 100 < 22
                 THEN 'discontinued' ELSE 'active' END                AS status,
            {pick(h_source, [(f"'{x}'", w) for x, w in SOURCES])}     AS source
        FROM _util.pim_skus k,
             LATERAL (SELECT unnest(generate_series(0, k.versions - 1)) AS k) v
    """)


def _arm(ctx: Context) -> None:
    """Apply the armed populations. Every one modifies a row that already
    exists, so the change count stays at exactly `CHANGES`."""
    s = ctx.seed
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.pim_armed AS
        SELECT
            v.* EXCLUDE (day_no, minute_no, source, status),
            -- A same-day double is dragged onto the previous version's day
            -- and keeps a later time, so the source clock ties on the date
            -- and the tie-break has to decide.
            CASE WHEN v.n > {_OUT_OF_ORDER_END} AND v.n <= {_DOUBLE_END}
                      AND v.k = {DOUBLE_VERSION}
                 THEN lag(v.day_no) OVER w
                 ELSE v.day_no END                                  AS day_no,
            CASE WHEN v.n > {_OUT_OF_ORDER_END} AND v.n <= {_DOUBLE_END}
                      AND v.k = {DOUBLE_VERSION}
                 THEN least(1439, lag(v.minute_no) OVER w + 137)
                 ELSE v.minute_no END                               AS minute_no,
            -- The 38 cross-source doubles are the pair the priority order
            -- decides; the other 58 come from one source and do not.
            CASE WHEN v.n > {_OUT_OF_ORDER_END} AND v.n <= {_DOUBLE_CROSS_END}
                      AND v.k = {DOUBLE_VERSION} - 1 THEN 'supplier_feed'
                 WHEN v.n > {_OUT_OF_ORDER_END} AND v.n <= {_DOUBLE_CROSS_END}
                      AND v.k = {DOUBLE_VERSION} THEN 'pim_ui'
                 WHEN v.n > {_DOUBLE_CROSS_END} AND v.n <= {_DOUBLE_END}
                      AND v.k IN ({DOUBLE_VERSION} - 1, {DOUBLE_VERSION})
                 THEN 'pim_ui'
                 ELSE v.source END                                  AS source,
            -- A delete closes a span. On 22 SKUs a later version brings the
            -- key back; on 142 more the delete is the end of the record.
            CASE WHEN v.n > {_DOUBLE_END} AND v.n <= {_RESURRECTION_END}
                      AND v.k = {DELETE_VERSION} THEN 'delete'
                 WHEN v.n > {_RESURRECTION_END} AND v.n <= {_DISCONTINUED_END}
                      AND v.k = v.versions - 1 THEN 'delete'
                 ELSE 'upsert' END                                  AS operation,
            CASE WHEN v.n > {_RESURRECTION_END} AND v.n <= {_DISCONTINUED_END}
                      AND v.k = v.versions - 1 THEN 'discontinued'
                 ELSE v.status END                                  AS status
        FROM _util.pim_versions v
        WINDOW w AS (PARTITION BY v.sku ORDER BY v.k)
    """)

    # The arrival clock. Every row lands in the next morning's file, and then
    # the out-of-order population is pushed past the versions that overtook
    # it: one version late for 139 of them, two for the one that has to land
    # inside a span already closed on both sides.
    # The feed minute is drawn per (SKU, day) and then stepped by the
    # version's place inside that day, so an ordinary arrival never overtakes
    # the version before it. Only the armed rows arrive out of order.
    h_recv = draw(s, "'pim_recv'", "a.sku", "a.day_no")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.pim_arrivals AS
        SELECT a.*,
               (DATE '{ctx.start}' + INTERVAL 1 DAY * a.day_no)::TIMESTAMP
                   + INTERVAL 1 MINUTE * a.minute_no              AS updated_at,
               (DATE '{ctx.start}' + INTERVAL 1 DAY * (a.day_no + 1))::TIMESTAMP
                   + INTERVAL {FEED_HOUR} HOUR
                   + INTERVAL 1 MINUTE * ((({h_recv}) % 40) + 5 * (
                       row_number() OVER (PARTITION BY a.sku, a.day_no
                                          ORDER BY a.minute_no, a.k) - 1))
                                                                  AS feed_at,
               CASE WHEN a.n <= {SPLIT_SPAN} AND a.k = {LATE_VERSION} THEN 2
                    WHEN a.n <= {_OUT_OF_ORDER_END} AND a.k = {LATE_VERSION} THEN 1
                    ELSE 0 END                                    AS overtaken_by
        FROM _util.pim_armed a
    """)
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.pim_received AS
        SELECT p.* EXCLUDE (feed_at, overtaken_by),
               CASE WHEN p.overtaken_by = 0 THEN p.feed_at
                    ELSE (lead(p.feed_at, p.overtaken_by)
                              OVER (PARTITION BY p.sku ORDER BY p.k)
                          + INTERVAL 1 MINUTE * 9) END            AS received_at
        FROM _util.pim_arrivals p
    """)


def _emit(ctx: Context) -> None:
    """`raw.pim_product_versions`, in the order the warehouse received it."""
    ctx.sql(f"""
        CREATE OR REPLACE TABLE raw.pim_product_versions AS
        SELECT
            'PIMC-' || lpad((row_number() OVER (
                ORDER BY received_at, sku, k))::VARCHAR, 7, '0')  AS change_id,
            sku,
            product_name,
            category_id::VARCHAR                                  AS category_id,
            brand,
            supplier_id,
            list_price_cents::BIGINT                              AS list_price_cents,
            status,
            '{{"color": "' || color || '", "size": "' || size || '"}}'
                                                                  AS attributes_json,
            operation,
            updated_at,
            received_at,
            source
        FROM _util.pim_received
        WHERE received_at < DATE '{ctx.today}'
    """)


def _categories(ctx: Context) -> None:
    """`raw.product_categories` — the tree the change feed's `category_id`
    joins to, effective-dated the way a reference extract is."""
    ctx.sql(f"""
        CREATE OR REPLACE TABLE raw.product_categories AS
        SELECT c.category_id::VARCHAR                             AS category_id,
               c.parent_category_id::VARCHAR                      AS parent_id,
               c.name,
               coalesce(c.dept_code, 'ALL')                       AS dept_code,
               DATE '{ctx.start}'                                 AS valid_from,
               NULL::DATE                                         AS valid_to
        FROM sim_product.categories c
        ORDER BY c.category_id
    """)


def _land_files(ctx: Context) -> None:
    """One file a day, holding what the feed delivered that day. RET-2 keeps
    Copperline's own landing files for 400 days."""
    start = _land.window_start(ctx, _land.OWN_DAYS)
    _land.write_hive(
        ctx,
        f"""
        SELECT received_at::DATE AS dt,
               change_id, sku, product_name, category_id, brand, supplier_id,
               list_price_cents, status, attributes_json, operation,
               updated_at, received_at, source
        FROM raw.pim_product_versions
        WHERE received_at >= DATE '{start}'
        ORDER BY received_at, change_id
        """,
        ctx.landing / "pim", "CSV", ("dt",), "changes", "HEADER")


def _check(ctx: Context) -> None:
    """The armed populations, counted the way a task counts them."""
    rows, skus = ctx.sql(
        "SELECT count(*), count(DISTINCT sku) FROM raw.pim_product_versions"
    ).fetchone()
    assert (rows, skus) == (CHANGES, SKUS), (rows, skus)

    # An out-of-order arrival is a row that landed after a version newer
    # than itself had already landed.
    late = ctx.sql("""
        SELECT count(*) FROM raw.pim_product_versions a
        WHERE EXISTS (SELECT 1 FROM raw.pim_product_versions b
                      WHERE b.sku = a.sku AND b.updated_at > a.updated_at
                        AND b.received_at < a.received_at)
    """).fetchone()[0]
    assert late == OUT_OF_ORDER, f"{late} out-of-order arrivals, not {OUT_OF_ORDER}"

    # One of them lands inside a span already closed on both sides: two later
    # versions had arrived before it did.
    split = ctx.sql("""
        SELECT count(*) FROM raw.pim_product_versions a
        WHERE (SELECT count(*) FROM raw.pim_product_versions b
               WHERE b.sku = a.sku AND b.updated_at > a.updated_at
                 AND b.received_at < a.received_at) >= 2
    """).fetchone()[0]
    assert split == SPLIT_SPAN, f"{split} arrivals split a closed span"

    doubles, cross = ctx.sql("""
        SELECT count(*), count(*) FILTER (WHERE sources > 1) FROM (
            SELECT sku, updated_at::DATE AS d, count(*) AS n,
                   count(DISTINCT source) AS sources
            FROM raw.pim_product_versions GROUP BY 1, 2 HAVING count(*) > 1)
    """).fetchone()
    assert doubles == DOUBLES, f"{doubles} same-day doubles, not {DOUBLES}"
    assert cross == DOUBLE_CROSS_SOURCE, f"{cross} cross-source, not {DOUBLE_CROSS_SOURCE}"

    resurrected = ctx.sql("""
        SELECT count(DISTINCT a.sku) FROM raw.pim_product_versions a
        WHERE a.operation = 'upsert'
          AND EXISTS (SELECT 1 FROM raw.pim_product_versions b
                      WHERE b.sku = a.sku AND b.operation = 'delete'
                        AND b.updated_at < a.updated_at)
    """).fetchone()[0]
    assert resurrected == RESURRECTIONS, \
        f"{resurrected} resurrections, not {RESURRECTIONS}"

    # Every cross-source double holds the strongest source and a weaker one,
    # so the tie-break has something to decide between.
    ties = ctx.sql(f"""
        SELECT count(*) FROM (
            SELECT sku, updated_at::DATE AS d,
                   bool_or(source = '{SOURCE_PRIORITY[0]}') AS has_top,
                   bool_or(source <> '{SOURCE_PRIORITY[0]}') AS has_lower
            FROM raw.pim_product_versions GROUP BY 1, 2 HAVING count(*) > 1)
        WHERE has_top AND has_lower
    """).fetchone()[0]
    assert ties == DOUBLE_CROSS_SOURCE, f"{ties} doubles the priority decides"

    orphan = ctx.sql("""
        SELECT count(*) FROM raw.pim_product_versions v
        LEFT JOIN raw.product_categories c USING (category_id)
        WHERE c.category_id IS NULL
    """).fetchone()[0]
    assert not orphan, f"{orphan} change(s) name a category nothing holds"


def build(ctx: Context) -> None:
    _skus(ctx)
    _versions(ctx)
    _arm(ctx)
    _emit(ctx)
    _categories(ctx)
    _check(ctx)
    _land_files(ctx)
