"""S3, S6a and S6b: the POS batches, the WMS files, the carrier feeds, and
`ops.load_control`.

The world is built once per module, from the real timeline at the small
profile, by calling the sim modules and the extract modules directly. The CLI
is exercised by `test_gen_copperline.py`; going through it again here would
cost a minute a test for the same tables.
"""

from __future__ import annotations

import datetime as dt

import pytest

from de_bench.tasks import repo_root

pytest.importorskip("duckdb")
import duckdb  # noqa: E402

WORLD = repo_root() / "worlds" / "copperline"
SMALL_DIVISOR = 20

E1_END = "2025-11-02"
E6_AT = "2025-11-03"
ARMED_FALLBACK = "2025-11-02"      # the US fall back, and the last day of E1
WINDOW = ("2026-02-01", "2026-05-01")
WINDOW_PACKAGES = 828_000


def _build(tmp_path, profile="small"):
    import sys

    sys.path.insert(0, str(repo_root() / "tools"))
    from gen_copperline import config
    from gen_copperline.extracts import calendars, carriers, ops_control, pos, wms
    from gen_copperline.upstream import (_util, core, customer, inventory, logistics,
                                         northwave, product, sales, store, supply_chain)

    cfg = config.load_timeline(WORLD / "timeline.yaml")
    tmp_path.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(tmp_path / "x3.duckdb"))
    ctx = config.Context(con=con, cfg=cfg, profile=profile, landing=tmp_path / "landing")
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    _util.ensure_days(ctx)
    for module in (core, product, store, northwave, customer, inventory,
                   supply_chain, logistics, sales):
        module.build(ctx)
    for module in (calendars, pos, wms, carriers, ops_control):
        module.build(ctx)
    return ctx


@pytest.fixture(scope="module")
def ctx(tmp_path_factory):
    built = _build(tmp_path_factory.mktemp("x3"))
    yield built
    built.con.close()


def one(ctx, sql):
    return ctx.sql(sql).fetchone()


def scalar(ctx, sql):
    return one(ctx, sql)[0]


# --- raw.stores ------------------------------------------------------------

def test_stores_is_an_scd2_of_290_rows(ctx):
    total, current, stores = one(ctx, """
        SELECT count(*), count(*) FILTER (WHERE is_current),
               count(DISTINCT store_id) FROM raw.stores
    """)
    assert (total, current, stores) == (290, 268, 268)

    # Every store has exactly one open-ended current version, and the prior
    # versions close the day before the next one opens.
    assert scalar(ctx, "SELECT count(*) FROM raw.stores "
                       "WHERE is_current AND valid_to IS NOT NULL") == 0
    assert scalar(ctx, """
        SELECT count(*) FROM raw.stores a JOIN raw.stores b
          ON b.store_id = a.store_id AND b.valid_from = a.valid_to + INTERVAL 1 DAY
        WHERE a.valid_to IS NOT NULL
    """) == 22

    # 62% of the estate sits in Americas zones. That share is the shape of
    # E1's error, so it is asserted rather than left to the draw.
    americas = scalar(ctx, "SELECT count(*) FROM raw.stores "
                           "WHERE is_current AND tz_name LIKE 'America/%'")
    assert americas == 166
    assert 0.61 < americas / 268 < 0.63

    # The acquired estate: 44 stores that opened before the acquisition.
    acquired, before = one(ctx, """
        SELECT count(*), count(*) FILTER (WHERE opened_on < DATE '2025-02-03')
        FROM raw.stores WHERE is_current AND acquired_from = 'northwave'
    """)
    assert (acquired, before) == (44, 44)

    # CMP-3's remodel span is a version of its own.
    assert one(ctx, """
        SELECT status, valid_from, valid_to FROM raw.stores
        WHERE store_id = 'S-0233' AND status = 'remodel'
    """) == ("remodel", dt.date(2026, 2, 3), dt.date(2026, 2, 28))


# --- the FY2023 slab -------------------------------------------------------

def test_the_slab_is_exactly_the_fy2023_year(ctx):
    n, stores, days, lo, hi = one(ctx, """
        SELECT count(*), count(DISTINCT store_id), count(DISTINCT business_date),
               min(business_date), max(business_date) FROM raw.pos_sales_daily
    """)
    assert (n, stores, days) == (70_861, 191, 371)
    assert (str(lo), str(hi)) == ("2023-01-29", "2024-02-03")
    assert 371 * 191 == 70_861

    # It does not scale with the profile: a comp answer has to exist at every
    # profile, and this is the only prior year there is.
    assert n == 70_861

    # Summary grain, and the arithmetic closes on every row.
    assert scalar(ctx, """
        SELECT count(*) FROM raw.pos_sales_daily
        WHERE net_cents <> gross_cents - discount_cents + tax_cents
           OR txn_count < 1 OR gross_cents < 1
    """) == 0

    # Every date the calendar names as an FY2024 comparable is in it.
    assert scalar(ctx, """
        SELECT count(*) FROM raw.fiscal_calendar c
        WHERE c.fiscal_year = 'FY2024' AND c.comp_date_ly IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM raw.pos_sales_daily s
                          WHERE s.business_date = c.comp_date_ly)
    """) == 0


# --- E1 and E6 -------------------------------------------------------------

def test_e1_stamps_local_and_e6_stamps_utc(ctx):
    # Through E1: one constant close-batch stamp, and no UTC at all.
    times, nulls, rows = one(ctx, f"""
        SELECT count(DISTINCT event_time_local::TIME),
               count(*) FILTER (WHERE event_time_utc IS NULL), count(*)
        FROM raw.pos_sales_header WHERE business_date <= DATE '{E1_END}'
    """)
    assert times == 1
    assert nulls == rows > 0
    assert scalar(ctx, f"SELECT DISTINCT event_time_local::TIME "
                       f"FROM raw.pos_sales_header "
                       f"WHERE business_date <= DATE '{E1_END}'") == dt.time(23, 5)

    # From E6: real per-transaction times, and UTC on every row.
    times, nulls, rows = one(ctx, f"""
        SELECT count(DISTINCT event_time_local::TIME),
               count(*) FILTER (WHERE event_time_utc IS NULL), count(*)
        FROM raw.pos_sales_header WHERE business_date >= DATE '{E6_AT}'
    """)
    assert times > 100
    assert (nulls, rows > 0) == (0, True)

    # And the UTC column is the local stamp read through the store's zone —
    # not the local stamp relabelled.
    assert scalar(ctx, f"""
        SELECT count(*) FROM raw.pos_sales_header h
        JOIN raw.stores s ON s.store_id = h.store_id AND s.is_current
        WHERE h.business_date >= DATE '{E6_AT}'
          AND h.event_time_utc
              <> (h.event_time_local AT TIME ZONE s.tz_name) AT TIME ZONE 'UTC'
    """) == 0


def test_reading_the_e1_stamp_as_utc_is_a_region_shaped_error(ctx):
    """The whole point of E1. Reading `event_time_local` as UTC moves the
    Americas stores' close batch into the next day and leaves the European
    ones alone, so the error is 62% of the estate and 0% of the rest."""
    wrong = ctx.sql(f"""
        SELECT s.tz_name LIKE 'America/%' AS americas,
               count(*) FILTER (WHERE h.business_date
                   <> ((h.event_time_local AT TIME ZONE s.tz_name)
                       AT TIME ZONE 'UTC')::DATE) AS moved,
               count(*) AS rows
        FROM raw.pos_sales_header h
        JOIN raw.stores s ON s.store_id = h.store_id AND s.is_current
        WHERE h.business_date <= DATE '{E1_END}'
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    europe, americas = wrong
    assert europe[1] == 0, "a UTC+0 or UTC+1 store's 23:05 is the same UTC day"
    assert americas[1] == americas[2], "every Americas batch moves a day"
    share = americas[2] / (americas[2] + europe[2])
    assert 0.58 < share < 0.66, share


def test_one_armed_dst_hour_is_genuinely_ambiguous(ctx):
    """On the two armed fall-backs the Americas clocks run 01:00 to 01:59
    twice, and the store server writes its local wall clock. So one
    `stamped_at` string in one zone can be two different real instants, which
    is what `received_at` records."""
    from gen_copperline.extracts.calendars import dst_transitions

    armed = [day for day, _which, state in dst_transitions("US") if state == "armed"]
    assert dt.date.fromisoformat(ARMED_FALLBACK) in armed

    collisions = ctx.sql(f"""
        SELECT s.tz_name, m.stamped_at, count(DISTINCT date_trunc('hour', m.received_at))
        FROM raw.pos_batch_manifest m
        JOIN raw.stores s ON s.store_id = m.store_id AND s.is_current
        WHERE m.stamped_at::DATE = DATE '{ARMED_FALLBACK}'
          AND m.stamped_at::TIME >= TIME '01:00' AND m.stamped_at::TIME < TIME '02:00'
          AND m.status <> 'missing'
        GROUP BY 1, 2 HAVING count(DISTINCT date_trunc('hour', m.received_at)) > 1
    """).fetchall()
    assert collisions, "no ambiguous stamp on the armed fall-back"

    # The pairs are exactly an hour apart in real time, which is the fold.
    assert scalar(ctx, f"""
        SELECT count(*) FROM (
            SELECT s.tz_name, m.stamped_at,
                   max(m.received_at) - min(m.received_at) AS spread
            FROM raw.pos_batch_manifest m
            JOIN raw.stores s ON s.store_id = m.store_id AND s.is_current
            WHERE m.stamped_at::DATE = DATE '{ARMED_FALLBACK}'
              AND m.stamped_at::TIME >= TIME '01:00'
              AND m.stamped_at::TIME < TIME '02:00'
            GROUP BY 1, 2 HAVING max(m.received_at) - min(m.received_at)
                                 >= INTERVAL 1 HOUR)
    """) > 0

    # Arizona never moves, so its stores are never ambiguous — a fix that
    # applies one rule to the whole estate is wrong for them too.
    assert scalar(ctx, f"""
        SELECT count(DISTINCT date_trunc('hour', m.received_at)) - count(DISTINCT m.stamped_at)
        FROM raw.pos_batch_manifest m
        JOIN raw.stores s ON s.store_id = m.store_id AND s.is_current
        WHERE s.tz_name = 'America/Phoenix' AND m.stamped_at::DATE = DATE '{ARMED_FALLBACK}'
    """) <= 0


# --- the manifest ----------------------------------------------------------

def test_the_manifest_counts_late_and_missing_batches(ctx):
    late, missing, total = one(ctx, """
        SELECT count(*) FILTER (WHERE status = 'late'),
               count(*) FILTER (WHERE status = 'missing'),
               count(*) FROM raw.pos_batch_manifest
    """)
    # The quota is handed out day by day, so the three all-market skip days
    # give up the batches they were owed: nothing trades, nothing is late.
    assert 6_400 <= late <= 6_500
    assert 980 <= missing <= 990
    assert 210_000 <= total <= 225_000

    # These are store-day counts, so they do not scale with the profile.
    assert ctx.profile == "small"

    # A missing batch means no rows at all, not a zero-sales day.
    assert scalar(ctx, """
        SELECT count(*) FROM raw.pos_batch_manifest m
        WHERE m.status = 'missing' AND EXISTS (
            SELECT 1 FROM raw.pos_sales_header h
            WHERE h.store_id = m.store_id AND h.business_date = m.business_date)
    """) == 0

    # The file lag is a lag on the file. Every row inside a late batch shares
    # its arrival, and no batch is more than three days late.
    assert scalar(ctx, """
        SELECT count(*) FROM raw.pos_batch_manifest
        WHERE status = 'late'
          AND (received_at::DATE - business_date < 1 OR received_at::DATE - business_date > 4)
    """) == 0
    assert scalar(ctx, """
        SELECT count(*) FROM (
            SELECT store_id, business_date FROM raw.pos_sales_header
            GROUP BY 1, 2 HAVING count(DISTINCT received_at) > 1)
    """) == 0

    # No batch is owed on a day the store's market sent nothing.
    assert scalar(ctx, """
        SELECT count(*) FROM raw.pos_batch_manifest m
        JOIN raw.stores s ON s.store_id = m.store_id AND s.is_current
        JOIN raw.market_calendar c
          ON c.market_code = s.market_code AND c.calendar_date = m.business_date
        WHERE NOT c.feed_expected
    """) == 0


def test_the_pos_header_populations(ctx):
    void, no_order, trade, rows = one(ctx, """
        SELECT count(*) FILTER (WHERE void_flag),
               count(*) FILTER (WHERE order_id IS NULL),
               count(*) FILTER (WHERE customer_ref IS NOT NULL),
               count(*) FROM raw.pos_sales_header
    """)
    assert 0.015 < void / rows < 0.021, void / rows
    assert 0.03 < no_order / rows < 0.05, no_order / rows
    assert trade > 0

    # The trade account number is written in the format of its era.
    assert scalar(ctx, """
        SELECT count(*) FROM raw.pos_sales_header
        WHERE customer_ref LIKE 'CUST%' AND business_date >= DATE '2024-11-04'
    """) == 0
    assert scalar(ctx, """
        SELECT count(*) FROM raw.pos_sales_header
        WHERE customer_ref LIKE 'C-%' AND business_date < DATE '2024-11-04'
    """) == 0

    # Integer cents that close, on every row.
    assert scalar(ctx, """
        SELECT count(*) FROM raw.pos_sales_header
        WHERE net_cents <> gross_cents - discount_cents + tax_cents
    """) == 0
    # Two clocks, ordered, no seconds.
    assert scalar(ctx, """
        SELECT count(*) FROM raw.pos_sales_header
        WHERE loaded_at < received_at OR loaded_at < event_time_utc
           OR date_part('second', event_time_utc) <> 0
    """) == 0


def test_the_pos_landing_tree_is_one_file_per_store_per_night(ctx):
    root = ctx.landing / "pos"
    assert root.is_dir()
    partitions = sorted(p.name for p in root.iterdir())
    assert f"dt={E6_AT}" in partitions and f"dt={E1_END}" in partitions

    # The all-market holiday keeps no partition: every market is shut, so
    # every store sends nothing and every ingest branch skips.
    assert "dt=2026-05-25" not in partitions
    assert "dt=2024-12-25" not in partitions

    files = list((root / f"dt={E6_AT}").glob("store_S-*.csv"))
    assert files
    header = files[0].read_text().splitlines()[0].split(",")
    assert "event_time_local" in header
    # The store server cannot compute UTC and does not send the warehouse's
    # own bookkeeping either.
    assert "event_time_utc" not in header
    assert "loaded_at" not in header and "received_at" not in header


# --- S6a, the WMS ----------------------------------------------------------

def test_the_valuation_change_flips_three_columns_on_one_day(ctx):
    before = one(ctx, """
        SELECT count(*) FILTER (WHERE unit_cost_cents IS NULL),
               count(*) FILTER (WHERE retail_value_cents IS NOT NULL),
               count(*) FILTER (WHERE cost_complement_bps IS NOT NULL), count(*)
        FROM raw.inventory_snapshots WHERE snapshot_date < DATE '2026-02-01'
    """)
    assert before[:3] == (0, 0, 0) and before[3] > 0

    after = one(ctx, """
        SELECT count(*) FILTER (WHERE unit_cost_cents IS NOT NULL),
               count(*) FILTER (WHERE retail_value_cents IS NULL),
               count(*) FILTER (WHERE cost_complement_bps IS NULL), count(*)
        FROM raw.inventory_snapshots WHERE snapshot_date >= DATE '2026-02-01'
    """)
    assert after[:3] == (0, 0, 0) and after[3] > 0

    # The complement is the only place the FY2026 rule's numbers are written.
    depts, periods = one(ctx, "SELECT count(DISTINCT dept_code), "
                              "count(DISTINCT fiscal_period) "
                              "FROM raw.dept_cost_complement")
    assert (depts, periods) == (5, 12)
    assert scalar(ctx, "SELECT count(*) FROM raw.dept_cost_complement "
                       "WHERE fiscal_period NOT LIKE 'FY2026-%'") == 0

    # Weighted-average cost understates a FY2026 department by 6 to 9 per
    # cent against the retail method, which is the whole valuation question.
    spread = ctx.sql("""
        SELECT dept_code,
               sum(retail_value_cents * cost_complement_bps / 10000.0)
                   / nullif(sum(on_hand_units * c.unit_cost), 0)
        FROM raw.inventory_snapshots s
        JOIN (SELECT sku, max(unit_cost_cents) AS unit_cost
              FROM raw.inventory_snapshots
              WHERE unit_cost_cents IS NOT NULL GROUP BY sku) c USING (sku)
        WHERE snapshot_date >= DATE '2026-02-01'
        GROUP BY 1
    """).fetchall()
    assert spread
    for dept, ratio in spread:
        assert 1.055 < ratio < 1.095, (dept, ratio)


def test_the_wms_speaks_its_own_vocabulary(ctx):
    words = {w for (w,) in ctx.sql(
        "SELECT DISTINCT movement_type FROM raw.wms_movements").fetchall()}
    assert words <= {"receipt", "pick", "pack", "adjust", "cycle_count", "rtv"}
    assert "pick" in words and "sale" not in words


# --- S6b, the carriers -----------------------------------------------------

def test_the_repricing_window_carries_its_package_volume(ctx):
    inside = scalar(ctx, f"""
        SELECT count(*) FROM raw.shipment_packages
        WHERE ship_date BETWEEN DATE '{WINDOW[0]}' AND DATE '{WINDOW[1]}'
    """)
    expected = WINDOW_PACKAGES / SMALL_DIVISOR
    assert 0.97 * expected < inside < 1.03 * expected, inside

    days = scalar(ctx, f"""
        SELECT count(DISTINCT ship_date) FROM raw.shipment_packages
        WHERE ship_date BETWEEN DATE '{WINDOW[0]}' AND DATE '{WINDOW[1]}'
    """)
    assert days == 90

    # Three to one against the rest of the range: that asymmetry is the whole
    # of the flagship's cost pressure.
    outside = scalar(ctx, f"""
        SELECT count(*) / count(DISTINCT ship_date) FROM raw.shipment_packages
        WHERE ship_date NOT BETWEEN DATE '{WINDOW[0]}' AND DATE '{WINDOW[1]}'
    """)
    assert 2.9 < (inside / days) / outside < 3.5

    # A shipment carries 2.2 packages, which is what the shipment target was
    # sized from and what makes the totals land where the spec puts them.
    assert 2.1 < scalar(ctx, """
        SELECT avg(n) FROM (SELECT count(*) AS n FROM raw.shipment_packages
                            GROUP BY shipment_id)
    """) < 2.3


def test_the_package_draw_is_anchored_on_the_upstream(ctx):
    """Every shipment the WMS knows about on a covered carrier is the same
    shipment in the carrier's file, by id."""
    assert scalar(ctx, """
        SELECT count(*) FROM raw.shipments
        WHERE shipment_id IN ('SH-3120884', 'SH-3119004', 'SH-3120770')
    """) == 3
    assert one(ctx, "SELECT carrier_code, ship_date FROM raw.shipments "
                    "WHERE shipment_id = 'SH-3120884'") == ("BRFR", dt.date(2026, 4, 15))
    # Every package belongs to a shipment, and every shipment to a lane.
    assert scalar(ctx, """
        SELECT count(*) FROM raw.shipment_packages p
        WHERE NOT EXISTS (SELECT 1 FROM raw.shipments s
                          WHERE s.shipment_id = p.shipment_id)
           OR NOT EXISTS (SELECT 1 FROM raw.lanes l WHERE l.lane_id = p.lane_id)
    """) == 0
    assert scalar(ctx, "SELECT count(*) FROM raw.lanes") == 44


def test_ship_date_is_local_at_origin(ctx):
    """`ship_date` and `ship_time_utc::DATE` disagree on a real population, and
    the disagreement is the origin's offset — which moves on the two 2026
    DST days, both inside the repricing window."""
    moved, rows = one(ctx, """
        SELECT count(*) FILTER (WHERE ship_date <> ship_time_utc::DATE), count(*)
        FROM raw.shipment_packages
    """)
    assert 0.10 < moved / rows < 0.35, moved / rows

    assert scalar(ctx, """
        SELECT count(*) FROM raw.shipment_packages p JOIN raw.lanes l USING (lane_id)
        WHERE p.ship_date
              <> ((p.ship_time_utc AT TIME ZONE 'UTC')
                  AT TIME ZONE l.origin_tz_name)::DATE
    """) == 0

    for day in ("2026-03-08", "2026-03-29"):
        assert scalar(ctx, f"SELECT count(*) FROM raw.shipment_packages "
                           f"WHERE ship_date = DATE '{day}'") > 0


def test_the_northwave_brand_is_derivable_and_never_stated(ctx):
    branded, rows = one(ctx, """
        SELECT count(*) FILTER (WHERE brand = 'northwave'), count(*)
        FROM raw.shipment_packages
    """)
    assert 0.10 < branded / rows < 0.22, branded / rows

    # The affected lane set is a fact about the data, not a column anywhere.
    lanes = {lane for (lane,) in ctx.sql(
        "SELECT DISTINCT lane_id FROM raw.shipment_packages "
        "WHERE brand = 'northwave'").fetchall()}
    origins = {origin for (origin,) in ctx.sql(
        f"SELECT DISTINCT origin_dc FROM raw.lanes WHERE lane_id IN "
        f"({', '.join(repr(lane) for lane in sorted(lanes))})").fetchall()}
    assert len(origins) == 1, origins
    assert scalar(ctx, """
        SELECT count(*) FROM raw.lanes
        WHERE lane_id NOT IN (SELECT lane_id FROM raw.shipment_packages
                              WHERE brand = 'northwave')
          AND origin_dc IN (SELECT origin_dc FROM raw.lanes l
                            WHERE l.lane_id IN (SELECT lane_id FROM raw.shipment_packages
                                                WHERE brand = 'northwave'))
    """) == 0


def test_billed_cents_is_the_rate_card_applied(ctx):
    """The hand-authored `raw.carrier_rate_cards` has to carry these numbers,
    so the formula is pinned here rather than left to whatever was drawn."""
    from gen_copperline.extracts import carriers as mod

    row = one(ctx, """
        SELECT carrier_code, service_level, zone, billed_weight_g, billed_cents
        FROM raw.shipment_packages ORDER BY package_id LIMIT 1
    """)
    carrier, service, zone, weight, billed = row
    weight_break = min(w for w in mod.WEIGHT_BREAKS_G if w >= weight) \
        if weight <= max(mod.WEIGHT_BREAKS_G) else max(mod.WEIGHT_BREAKS_G)
    assert billed == (mod.RATE_BASE_CENTS[service]
                      + zone * mod.ZONE_STEP_CENTS[carrier]
                      + (weight_break // 1000) * mod.WEIGHT_STEP_CENTS[service])

    assert scalar(ctx, "SELECT count(*) FROM raw.shipment_packages "
                       "WHERE billed_cents <= 0 OR accessorial_cents < 0") == 0

    # Six carrier-service pairs, which is what the card is sized on: two
    # versions by six pairs by 44 lanes by five weight breaks is 2,640 rows.
    assert scalar(ctx, "SELECT count(*) FROM (SELECT DISTINCT carrier_code, "
                       "service_level FROM raw.shipment_packages)") == 6
    assert scalar(ctx, "SELECT count(*) FROM raw.carrier_invoices") == 90
    assert scalar(ctx, """
        SELECT count(DISTINCT carrier_code) FROM raw.carrier_invoices
    """) == 3


def test_the_generator_never_writes_the_hand_authored_rate_cards(ctx):
    """Extract convention 7: the rate cards ship as source, so the generator
    must contain no reference to them."""
    package = repo_root() / "tools" / "gen_copperline"
    offenders = [
        str(path.relative_to(package))
        for path in package.rglob("*.py")
        if "raw.carrier_rate_cards" in path.read_text()
        and "hand-authored" not in path.read_text()
    ]
    assert not offenders, offenders
    assert not ctx.sql(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'raw' AND table_name = 'carrier_rate_cards'"
    ).fetchall()


# --- ops.load_control ------------------------------------------------------

def test_the_legacy_control_chain_breaks_on_the_sixth(ctx):
    before = scalar(ctx, "SELECT count(DISTINCT job_name) FROM ops.load_control "
                         "WHERE business_date < DATE '2026-01-05'")
    after = scalar(ctx, "SELECT count(DISTINCT job_name) FROM ops.load_control "
                        "WHERE business_date >= DATE '2026-01-05'")
    assert (before, after) == (22, 13), "wave 1 moved nine of the twenty-two"

    # The variable stops being produced on the sixth; the sixth still runs.
    assert scalar(ctx, """
        SELECT count(*) FROM ops.load_control
        WHERE business_date < DATE '2026-04-06' AND watermark_column IS NULL
    """) == 0
    assert scalar(ctx, """
        SELECT count(*) FROM ops.load_control
        WHERE business_date >= DATE '2026-04-06' AND watermark_column IS NOT NULL
    """) == 0
    assert scalar(ctx, """
        SELECT count(*) FILTER (WHERE load_status <> 'complete')
        FROM ops.load_control WHERE business_date = DATE '2026-04-06'
    """) == 0
    assert scalar(ctx, """
        SELECT count(*) FILTER (WHERE load_status <> 'failed')
        FROM ops.load_control WHERE business_date > DATE '2026-04-06'
    """) == 0
    assert scalar(ctx, """
        SELECT count(*) FROM ops.load_control
        WHERE load_status = 'failed' AND (ended_at IS NOT NULL OR history_complete)
    """) == 0


# --- determinism -----------------------------------------------------------

def test_two_builds_agree_row_for_row(tmp_path):
    def fingerprint(built):
        out = {}
        for schema, table in built.sql(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema IN ('raw', 'ops') ORDER BY 1, 2"
        ).fetchall():
            out[f"{schema}.{table}"] = built.sql(
                f"SELECT count(*), coalesce(sum(hash(t)), 0) FROM {schema}.{table} t"
            ).fetchone()
        return out

    first = _build(tmp_path / "a")
    second = _build(tmp_path / "b")
    try:
        assert fingerprint(first) == fingerprint(second)
        assert len(fingerprint(first)) >= 10
        # The landing tree is a fixture too, byte for byte.
        for name in ("pos", "wms", "carriers"):
            left = sorted(p.relative_to(first.landing) for p
                          in (first.landing / name).rglob("*.csv"))
            right = sorted(p.relative_to(second.landing) for p
                           in (second.landing / name).rglob("*.csv"))
            assert left == right and left
            assert (first.landing / left[0]).read_bytes() \
                == (second.landing / left[0]).read_bytes()
    finally:
        first.con.close()
        second.con.close()
