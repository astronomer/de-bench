"""The four extract modules that land the events, the marketing feeds, the
catalog change feed and the CRM books: `clickstream`, `ads`, `pim`, `crm`.

Each one reads `sim_*` and writes `raw.*`, `ops.*` and the landing tree, so
the tests build the upstream they need for real and stub only `sim_sales`,
which the sales module owns and which is built elsewhere. Building through
the CLI would cost eighty seconds a case; a Context against an in-memory
database costs under two.
"""

import datetime as dt
import sys
from pathlib import Path

import duckdb
import pytest

from de_bench.tasks import repo_root

sys.path.insert(0, str(repo_root() / "tools"))

from gen_copperline import build as orchestrator          # noqa: E402
from gen_copperline import config, upstream               # noqa: E402
from gen_copperline.extracts import ads, clickstream, crm, pim  # noqa: E402
from gen_copperline.upstream import external, northwave, product  # noqa: E402

WORLD = repo_root() / "worlds" / "copperline"

# What the stub stands in for: the order spine the clickstream's `purchase`
# events point at. Spec 02 sections 6 and 8.
_STUB = """
CREATE SCHEMA IF NOT EXISTS sim_store;
CREATE SCHEMA IF NOT EXISTS sim_sales;

CREATE OR REPLACE TABLE sim_store.channels AS
SELECT * FROM (VALUES
    (1, 'Retail Store', 'store', true), (2, 'Web Store', 'web', true),
    (3, 'Marketplace', 'marketplace', true), (4, 'Trade', 'trade', true)
) t(channel_id, name, channel_type, is_active);

CREATE OR REPLACE TABLE sim_sales.payment_methods AS
SELECT * FROM (VALUES
    (1, 'Visa Credit', 'card', 'Meridian Pay', 2, 0.0229),
    (2, 'Mastercard',  'card', 'Meridian Pay', 2, 0.0215),
    (3, 'Cash',        'cash', 'Copperline',   0, 0.0000)
) t(payment_method_id, name, method_type, provider, settlement_days, fee_pct);

CREATE OR REPLACE TABLE sim_sales.orders AS
SELECT d.daynum * 1000 + g.i                              AS order_id,
       'W-' || (d.daynum * 1000 + g.i)::VARCHAR           AS order_number,
       400000 + (d.daynum * 1000 + g.i) % 5000            AS customer_id,
       1 + g.i % 4                                        AS channel_id,
       d.ds::TIMESTAMP + INTERVAL 1 HOUR * (9 + g.i % 9)  AS order_datetime,
       'USD'                                              AS currency_code,
       round(20 + (g.i * 37 % 900) / 3.0, 2)::DECIMAL(18,4) AS grand_total
FROM (SELECT range::DATE AS ds,
             date_diff('day', DATE '1970-01-01', range::DATE) AS daynum
      FROM range(DATE '{start}', DATE '{end}' + INTERVAL 1 DAY, INTERVAL 1 DAY)) d
CROSS JOIN generate_series(1, 12) g(i);

CREATE OR REPLACE TABLE sim_sales.payments AS
SELECT o.order_id AS payment_id, o.order_id,
       1 + o.order_id % 2 AS payment_method_id, o.grand_total AS amount,
       o.currency_code, 'captured' AS status,
       o.order_datetime AS authorized_at,
       o.order_datetime + INTERVAL 3 HOUR AS captured_at,
       'mp_' || lpad(o.order_id::VARCHAR, 12, '0') AS gateway_reference
FROM sim_sales.orders o
JOIN sim_store.channels ch ON ch.channel_id = o.channel_id
WHERE ch.channel_type <> 'trade';
"""


def _context(tmp_path: Path, profile: str = "small",
             volumes: dict | None = None) -> config.Context:
    cfg = config.load_timeline(WORLD / "timeline.yaml")
    if volumes:
        cfg["volumes"].update(volumes)
    con = duckdb.connect()
    ctx = config.Context(con=con, cfg=cfg, profile=profile,
                         landing=tmp_path / "landing")
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    orchestrator._build_days(ctx)
    product.build(ctx)
    northwave.build(ctx)
    con.execute(_STUB.format(start=ctx.start, end=ctx.end))
    external.build(ctx)
    return ctx


@pytest.fixture(scope="module")
def small(tmp_path_factory):
    ctx = _context(tmp_path_factory.mktemp("small"))
    for module in (clickstream, ads, pim, crm):
        module.build(ctx)
    return ctx


@pytest.fixture(scope="module")
def real_upstream(tmp_path_factory):
    """The whole of `upstream.ORDER`, built for real, then `crm` on top.

    The stub above stands in for one module. This fixture stands in for
    none: `sim_customer` and `sim_support` are the two the CRM extract reads
    where they exist, and their real shape is what a standalone test cannot
    see. It costs about five seconds at the small profile, so it is built
    once for the module.
    """
    cfg = config.load_timeline(WORLD / "timeline.yaml")
    con = duckdb.connect()
    ctx = config.Context(con=con, cfg=cfg, profile="small",
                         landing=tmp_path_factory.mktemp("real") / "landing")
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    orchestrator._build_days(ctx)
    for module in upstream.ORDER:
        module.build(ctx)
    crm.build(ctx)
    return ctx


# --- S4, the clickstream ----------------------------------------------------


def test_the_scale_knob_lives_in_the_timeline_and_ships_at_a_quarter():
    """The one volume with a knob of its own. Spec 03 section 9 runs the
    shipped profile at 0.25 and section 20 sizes the fixture from it."""
    cfg = config.load_timeline(WORLD / "timeline.yaml")
    assert cfg["volumes"]["clickstream_scale"] == 0.25
    assert clickstream.SHIPPED_SCALE == 0.25


def test_the_scale_knob_moves_the_feed_and_nothing_else(tmp_path):
    quarter = _context(tmp_path / "quarter")
    clickstream.build(quarter)
    half = _context(tmp_path / "half", volumes={"clickstream_scale": 0.5})
    clickstream.build(half)

    def rows(ctx):
        return ctx.sql("SELECT count(*) FROM raw.web_events").fetchone()[0]

    assert 1.8 < rows(half) / rows(quarter) < 2.2

    # The armed populations scale with it and stay exactly proportional.
    for ctx, factor in ((quarter, 1), (half, 2)):
        replayed = ctx.sql(
            f'SELECT count(*) FROM raw.web_events WHERE "offset" >= '
            f"{clickstream.REPLAY_OFFSET_BASE}").fetchone()[0]
        assert replayed > 0
        duplicated = ctx.sql(
            "SELECT count(*) FROM (SELECT event_id FROM raw.web_events "
            "GROUP BY 1 HAVING count(*) > 1)").fetchone()[0]
        assert duplicated * 2 < replayed  # most of a replay is new to us


def test_the_producer_lag_split_is_measurable(small):
    """The bound is four hours for the live producers and twenty-six for the
    nightly batch, and a task has to find both from the data. Replayed rows
    carry the replay's clock and sit in their own offset band."""
    lags = dict(small.sql(f"""
        SELECT producer, max(datediff('minute', event_time_utc, load_time))
        FROM raw.web_events WHERE "offset" < {clickstream.REPLAY_OFFSET_BASE}
        GROUP BY 1
    """).fetchall())
    assert set(lags) == {"web", "ios", "android", "email_click", "nightly_batch"}
    for producer in ("web", "ios", "android", "email_click"):
        assert 120 < lags[producer] <= 240, (producer, lags[producer])
    assert 1200 <= lags["nightly_batch"] <= 26 * 60

    # Nothing is loaded before it happened, and no event time carries seconds.
    assert small.sql("""
        SELECT count(*) FROM raw.web_events
        WHERE load_time < event_time_utc
           OR date_part('second', event_time_utc) <> 0
    """).fetchone()[0] == 0


def test_there_is_no_clickstream_from_the_marketplace(small):
    """Spec 03 sections 9 and 19 both say so, because it is easy to add by
    accident and it stops any task assuming one funnel covers all three
    channels."""
    producers = {p for (p,) in small.sql(
        "SELECT DISTINCT producer FROM raw.web_events").fetchall()}
    assert producers == {p for p, _ in clickstream.PRODUCERS}
    assert "marketplace" not in producers


def test_the_replay_is_the_population_the_incident_note_counts(small):
    """`ops/incidents/2026-01-23-event-replay.md` ships inside the world and
    prints three numbers. They are the fixture's numbers."""
    scale = clickstream.scale_of(small)
    expect_total = clickstream._at_profile(
        small, clickstream.REPLAY_TOTAL, scale)
    expect_dup = clickstream._at_profile(
        small, clickstream.REPLAY_DUPLICATES, scale)

    replayed, = small.sql(
        f'SELECT count(*) FROM raw.web_events WHERE "offset" >= '
        f"{clickstream.REPLAY_OFFSET_BASE}").fetchone()
    assert abs(replayed - expect_total) <= 3

    duplicated, = small.sql("""
        SELECT count(*) FROM (SELECT event_id FROM raw.web_events
                              GROUP BY 1 HAVING count(*) > 1)
    """).fetchone()
    assert abs(duplicated - expect_dup) <= 3

    # A duplicate keeps its event_id and its event time, and takes a new
    # offset and a new load time. That is the whole of EVT-1.
    same, moved = small.sql("""
        SELECT count(*) FILTER (WHERE times = 1), count(*) FILTER (WHERE offsets > 1)
        FROM (SELECT event_id, count(DISTINCT event_time_utc) AS times,
                     count(DISTINCT "offset") AS offsets
              FROM raw.web_events GROUP BY 1 HAVING count(*) > 1)
    """).fetchone()
    assert same == duplicated and moved == duplicated

    # Every replayed row landed inside the replay window and belongs to the
    # decay window it recovered.
    stray = small.sql(f"""
        SELECT count(*) FROM raw.web_events
        WHERE "offset" >= {clickstream.REPLAY_OFFSET_BASE}
          AND (load_time::DATE NOT BETWEEN DATE '{clickstream.REPLAY_WINDOW[0]}'
                                       AND DATE '{clickstream.REPLAY_WINDOW[1]}'
               OR event_time_utc::DATE NOT BETWEEN DATE '{clickstream.DECAY_WINDOW[0]}'
                                               AND DATE '{clickstream.DECAY_WINDOW[1]}')
    """).fetchone()[0]
    assert not stray


def test_the_decay_days_hold_the_postmortem_shape(small):
    """`ops/incidents/2026-01-14-feed-decay.md` prints a daily table and a
    90-day baseline. Both are graded material for NLO-1."""
    counts = dict(small.sql(f"""
        SELECT event_time_utc::DATE, count(*) FROM raw.web_events
        WHERE "offset" < {clickstream.REPLAY_OFFSET_BASE}
          AND event_time_utc::DATE BETWEEN DATE '2026-01-09' AND DATE '2026-01-15'
        GROUP BY 1
    """).fetchall())
    assert counts.get(dt.date(2026, 1, 14), 0) == 0, "the hard failure day"
    falls = [counts[dt.date(2026, 1, d)] for d in (10, 11, 12, 13)]
    assert falls == sorted(falls, reverse=True), falls
    # Three consecutive falls, each one small enough to sit inside a daily
    # two-sigma band, and cumulatively about 40%.
    for before, after in zip(falls, falls[1:]):
        assert 0.10 < 1 - after / before < 0.22
    assert 0.35 < 1 - falls[-1] / falls[0] < 0.45


def test_the_ninety_day_baseline_is_the_one_the_postmortem_states(tmp_path):
    """Mean 3,100, standard deviation 250, lowest 2,510, highest 3,760, and
    four days outside a two-sigma band. The shipped profile is the one the
    document describes, so this case runs at it."""
    ctx = _context(tmp_path, profile="full")
    clickstream.build(ctx)
    mean, sd, low, high, days = ctx.sql(f"""
        SELECT avg(n), stddev(n), min(n), max(n), count(*) FROM (
            SELECT event_time_utc::DATE AS d, count(*) AS n
            FROM raw.web_events
            WHERE "offset" < {clickstream.REPLAY_OFFSET_BASE}
              AND event_time_utc::DATE BETWEEN DATE '2025-10-13'
                                           AND DATE '2026-01-10'
            GROUP BY 1)
    """).fetchone()
    assert days == 90
    assert 3_000 <= mean <= 3_200
    assert 200 <= sd <= 300
    assert (low, high) == (2_510, 3_760)

    outside = ctx.sql(f"""
        SELECT count(*) FROM (
            SELECT event_time_utc::DATE AS d, count(*) AS n
            FROM raw.web_events
            WHERE "offset" < {clickstream.REPLAY_OFFSET_BASE}
              AND event_time_utc::DATE BETWEEN DATE '2025-10-13'
                                           AND DATE '2026-01-10'
            GROUP BY 1)
        WHERE abs(n - 3100) > 500
    """).fetchone()[0]
    assert outside == 4, "the two known peaks and the two regional closures"


def test_the_hourly_parquet_holds_one_day_hour_producer(small, tmp_path):
    files = sorted((small.landing / "events").rglob("*.parquet"))
    assert files, "the collector wrote nothing"
    assert {f.name for f in files} == {"part-000.parquet"}

    one = files[len(files) // 2]
    dt_part, hr_part, producer_part = (p.name for p in
                                       (one.parents[2], one.parents[1],
                                        one.parents[0]))
    assert dt_part.startswith("dt=") and hr_part.startswith("hr=")
    assert producer_part.startswith("producer=")
    assert len(hr_part) == len("hr=00")

    con = duckdb.connect()
    dts, hrs, producers, rows = con.execute(f"""
        SELECT count(DISTINCT load_time::DATE),
               count(DISTINCT date_part('hour', load_time)),
               count(DISTINCT producer), count(*)
        FROM read_parquet('{one}')
    """).fetchone()
    con.close()
    assert (dts, hrs, producers) == (1, 1, 1)
    assert rows > 0
    assert dt_part == f"dt={one.parents[2].name.split('=')[1]}"

    # The tree is the trailing window, not the whole history.
    dates = {f.parents[2].name for f in files}
    assert len(dates) <= clickstream.LANDING_DAYS


def test_the_feeds_are_absent_on_the_days_the_calendar_says_so(small):
    """`ops/calendar/quiet-days.yml`: the feeds pause from 18:00 on the
    Friday and the clickstream is absent, not late, for two days."""
    for day in ("2026-03-21", "2026-03-22"):
        assert small.sql("SELECT count(*) FROM raw.web_events WHERE "
                         f"event_time_utc::DATE = DATE '{day}'").fetchone()[0] == 0
    last = small.sql("SELECT max(date_part('hour', event_time_utc)) "
                     "FROM raw.web_events WHERE event_time_utc::DATE = "
                     "DATE '2026-03-20'").fetchone()[0]
    assert last < clickstream.PAUSE_FROM[1]


# --- S7, the marketing platforms --------------------------------------------


def test_the_ad_platforms_restate_with_moved_numbers(small):
    """Every delivery is kept, so reading the table without taking the latest
    per key double-counts spend. Spec 03 section 12."""
    keys, deliveries, restated = small.sql("""
        SELECT count(DISTINCT (report_date, platform, campaign_id)),
               count(*), count(*) FILTER (WHERE restated_at IS NOT NULL)
        FROM raw.ads_spend_daily
    """).fetchone()
    assert restated > 0
    assert deliveries == keys + restated, "a delivery went missing"

    # A restated key is delivered twice, the second time three to seven days
    # after the report date, and the numbers move.
    moved, total = small.sql("""
        SELECT count(*) FILTER (WHERE spends > 1), count(*) FROM (
            SELECT report_date, platform, campaign_id,
                   count(*) AS n, count(DISTINCT spend_cents) AS spends
            FROM raw.ads_spend_daily GROUP BY 1, 2, 3 HAVING count(*) > 1)
    """).fetchone()
    assert total == restated
    assert moved > total * 0.9, "restatements that do not move are not restatements"

    windows = small.sql("""
        SELECT min(datediff('day', report_date, restated_at::DATE)),
               max(datediff('day', report_date, restated_at::DATE))
        FROM raw.ads_spend_daily WHERE restated_at IS NOT NULL
    """).fetchone()
    assert windows == (3, 7)

    # The first delivery of a restated key is still there, with a NULL
    # `restated_at` and the earlier `loaded_at`.
    first, second = small.sql("""
        SELECT count(*) FILTER (WHERE restated_at IS NULL),
               count(*) FILTER (WHERE restated_at IS NOT NULL)
        FROM raw.ads_spend_daily
        WHERE (report_date, platform, campaign_id) IN (
            SELECT report_date, platform, campaign_id FROM raw.ads_spend_daily
            WHERE restated_at IS NOT NULL)
    """).fetchone()
    assert first == second == restated


def test_ad_money_is_integer_cents_in_the_market_currency(small):
    """Spec 03 section 1 rule 1. There is no `fx_rate_ppm` on this table: the
    join through `raw.fx_rates` is the work the currency column arms."""
    columns = {c for (c,) in small.sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = 'ads_spend_daily'"
    ).fetchall()}
    assert {"spend_cents", "attributed_revenue_cents", "currency_code"} <= columns
    assert "fx_rate_ppm" not in columns
    assert not {c for c in columns if c.endswith(("_amount", "_revenue"))}

    types = dict(small.sql(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = 'ads_spend_daily' "
        "AND column_name LIKE '%_cents'").fetchall())
    assert set(types.values()) == {"BIGINT"}

    currencies = {c for (c,) in small.sql(
        "SELECT DISTINCT currency_code FROM raw.ads_spend_daily").fetchall()}
    assert len(currencies) > 1, "the platforms bill in the market's currency"


def test_the_ad_landing_files_are_one_per_platform_per_delivery_day(small):
    files = sorted((small.landing / "ads").rglob("*.csv"))
    assert files
    names = {f.name for f in files}
    assert names == {f"{p}.csv" for p in ads.SPEND_PLATFORMS} | {"larkspur.csv"}
    dates = {f.parent.name for f in files}
    assert len(dates) <= 90, "RET-1 keeps a vendor landing file 90 days"

    # A restatement turns up in a later day's file than its original, which
    # is how the same (platform, report_date) comes to be in the tree twice.
    con = duckdb.connect()
    both = con.execute(f"""
        SELECT count(*) FROM (
            SELECT report_date, campaign_id, count(DISTINCT dt) AS files
            FROM read_csv_auto('{small.landing / "ads"}/dt=*/beacon.csv',
                               hive_partitioning = true)
            GROUP BY 1, 2 HAVING count(DISTINCT dt) > 1)
    """).fetchone()[0]
    con.close()
    assert both > 0


def test_larkspur_reports_events_and_never_spend(small):
    assert small.sql("SELECT count(*) FROM raw.ads_spend_daily "
                     "WHERE platform = 'larkspur'").fetchone()[0] == 0
    assert small.sql("SELECT count(*) FROM raw.email_events").fetchone()[0] > 0
    # The hash is derived from the address, and the address never lands.
    columns = {c for (c,) in small.sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = 'email_events'").fetchall()}
    assert "email_hash" in columns and "email" not in columns


# --- S9, the PIM change feed ------------------------------------------------


def test_the_pim_populations_are_exact_at_every_profile(tmp_path):
    """Spec 03 section 14: the edge populations carry the task and are not
    scaled. 21,000 rows cost nothing to build, so they are exact at the small
    profile too, and this case proves it at both."""
    for profile in ("small", "full"):
        ctx = _context(tmp_path / profile, profile=profile)
        pim.build(ctx)

        rows, skus = ctx.sql(
            "SELECT count(*), count(DISTINCT sku) FROM raw.pim_product_versions"
        ).fetchone()
        assert (rows, skus) == (pim.CHANGES, pim.SKUS), profile

        late = ctx.sql("""
            SELECT count(*) FROM raw.pim_product_versions a
            WHERE EXISTS (SELECT 1 FROM raw.pim_product_versions b
                          WHERE b.sku = a.sku AND b.updated_at > a.updated_at
                            AND b.received_at < a.received_at)
        """).fetchone()[0]
        assert late == pim.OUT_OF_ORDER == 140, profile

        doubles, cross = ctx.sql("""
            SELECT count(*), count(*) FILTER (WHERE sources > 1) FROM (
                SELECT sku, updated_at::DATE, count(DISTINCT source) AS sources
                FROM raw.pim_product_versions
                GROUP BY 1, 2 HAVING count(*) > 1)
        """).fetchone()
        assert (doubles, cross) == (96, 38), profile

        resurrected = ctx.sql("""
            SELECT count(DISTINCT a.sku) FROM raw.pim_product_versions a
            WHERE a.operation = 'upsert'
              AND EXISTS (SELECT 1 FROM raw.pim_product_versions b
                          WHERE b.sku = a.sku AND b.operation = 'delete'
                            AND b.updated_at < a.updated_at)
        """).fetchone()[0]
        assert resurrected == pim.RESURRECTIONS == 22, profile


def test_one_out_of_order_arrival_lands_inside_a_closed_span(small):
    """139 of them land inside the span that was open when they arrived. One
    lands with two later versions already either side of it, which a naive
    incremental build truncates instead of splicing."""
    split = small.sql("""
        SELECT count(*) FROM raw.pim_product_versions a
        WHERE (SELECT count(*) FROM raw.pim_product_versions b
               WHERE b.sku = a.sku AND b.updated_at > a.updated_at
                 AND b.received_at < a.received_at) >= 2
    """).fetchone()[0]
    assert split == pim.SPLIT_SPAN == 1


def test_a_delete_does_not_mean_a_resurrection(small):
    """`operation = 'delete'` has to be commoner than the resurrections, or
    the population gives itself away."""
    deletes = small.sql("SELECT count(*) FROM raw.pim_product_versions "
                        "WHERE operation = 'delete'").fetchone()[0]
    assert deletes == pim.RESURRECTIONS + pim.DISCONTINUATIONS
    assert deletes > pim.RESURRECTIONS * 4


def test_the_pim_change_feed_lands_one_file_a_day(small):
    files = sorted((small.landing / "pim").rglob("*.csv"))
    assert files
    assert {f.name for f in files} == {"changes.csv"}
    dates = {f.parent.name for f in files}
    assert len(dates) == len(files), "one file per date"
    assert len(dates) <= 400, "RET-2 keeps our own landing files 400 days"

    con = duckdb.connect()
    days = con.execute(
        f"SELECT count(DISTINCT received_at::DATE) FROM read_csv_auto('{files[-1]}')"
    ).fetchone()[0]
    con.close()
    assert days == 1


def test_every_pim_change_names_a_category_that_exists(small):
    assert small.sql("""
        SELECT count(*) FROM raw.pim_product_versions v
        LEFT JOIN raw.product_categories c USING (category_id)
        WHERE c.category_id IS NULL
    """).fetchone()[0] == 0


# --- S8, the CRM books ------------------------------------------------------


def test_the_two_books_and_the_three_hundred_decided_pairs(small):
    total, active = small.sql(
        "SELECT count(*), count(*) FILTER (WHERE status = 'active') "
        "FROM raw.customers").fetchone()
    assert (total, active) == (4_000, 3_960)

    total, active = small.sql(
        "SELECT count(*), count(*) FILTER (WHERE status = 'active') "
        "FROM raw.nwv_accounts").fetchone()
    assert (total, active) == (1_400, 1_380)

    assert small.sql(
        "SELECT count(*) FROM ops.merge_candidates").fetchone()[0] == 300


def test_the_sixty_decoys_never_reach_the_flagged_table(small):
    """Their absence is what makes `ops.merge_candidates` trustworthy and a
    fuzzy answer wrong. Spec 03 section 13."""
    decoys = small.sql("""
        SELECT count(*) FROM sim_identity.merge_truth m
        JOIN _util.trade_customer_ids c ON c.trade_seq = m.trade_seq
        WHERE NOT m.is_true_pair
          AND (c.customer_id, m.nwa_id) IN
              (SELECT customer_id, nwv_account_id FROM ops.merge_candidates)
    """).fetchone()[0]
    assert decoys == 0
    assert small.sql("SELECT count(*) FROM sim_identity.merge_truth "
                     "WHERE NOT is_true_pair").fetchone()[0] == 60


def test_both_id_formats_reach_party_ref(small):
    """`party_ref` holds a `C-` id or an `NWA-` id and only `party_source`
    says which book to look in. Swap the books and every total holds."""
    counts = dict(small.sql(
        "SELECT party_source, count(*) FROM raw.support_tickets GROUP BY 1"
    ).fetchall())
    assert set(counts) == {"oms", "nwv"}
    assert min(counts.values()) > 0

    shapes = small.sql("""
        SELECT count(*) FILTER (WHERE party_source = 'oms'
                                  AND party_ref NOT LIKE 'C-%'),
               count(*) FILTER (WHERE party_source = 'nwv'
                                  AND party_ref NOT LIKE 'NWA-%')
        FROM raw.support_tickets
    """).fetchone()
    assert shapes == (0, 0)


def test_the_party_pair_is_derived_from_the_real_service_desk(real_upstream):
    """The break integration found. `sim_support.tickets` carries a
    `customer_id` and no party columns, so both armed columns are made at
    this boundary — and the acquired book's tickets, which `sim_support` does
    not simulate at all, are augmented in beside them."""
    ctx = real_upstream
    columns = {c for (c,) in ctx.sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'sim_support' AND table_name = 'tickets'"
    ).fetchall()}
    assert "customer_id" in columns
    assert not {"party_ref", "party_source"} & columns, \
        "the upstream grew party columns; read them instead of deriving them"

    counts = dict(ctx.sql(
        "SELECT party_source, count(*) FROM raw.support_tickets GROUP BY 1"
    ).fetchall())
    assert set(counts) == {"oms", "nwv"}
    assert min(counts.values()) > 0

    # Every Copperline ticket the real desk holds reaches the extract, with
    # the code the customer module minted for it.
    real, extracted = ctx.sql("""
        SELECT (SELECT count(*) FROM sim_support.tickets),
               (SELECT count(*) FROM raw.support_tickets WHERE party_source = 'oms')
    """).fetchone()
    assert real == extracted
    assert ctx.sql("""
        SELECT count(*) FROM raw.support_tickets t
        WHERE t.party_source = 'oms'
          AND t.party_ref NOT IN (SELECT customer_code FROM sim_customer.customers)
    """).fetchone()[0] == 0

    # Every acquired-book reference resolves against its own book, and the
    # trade accounts in the Copperline book are reachable too. A shopper's
    # code resolves against nothing, because `raw.customers` is the trade
    # book — spec 03 section 13 says so outright.
    assert ctx.sql("""
        SELECT count(*) FROM raw.support_tickets t
        WHERE t.party_source = 'nwv'
          AND t.party_ref NOT IN (SELECT nwv_account_id FROM raw.nwv_accounts)
    """).fetchone()[0] == 0
    assert ctx.sql("""
        SELECT count(*) FROM raw.support_tickets t
        JOIN raw.customers c ON c.customer_id = t.party_ref
    """).fetchone()[0] > 0

    # The two legs share one vocabulary, so a drawn ticket cannot be picked
    # out of the feed by the words in it.
    drawn, whole = ctx.sql("""
        SELECT count(DISTINCT category) FILTER (WHERE party_source = 'nwv'),
               count(DISTINCT category)
        FROM raw.support_tickets
    """).fetchone()
    assert 0 < drawn <= whole
    assert ctx.sql("""
        SELECT count(*) FROM raw.support_tickets
        WHERE category NOT IN (SELECT name FROM sim_support.ticket_categories)
    """).fetchone()[0] == 0

    # Only Copperline's orders ever reached the warehouse.
    assert ctx.sql("SELECT count(order_id) FROM raw.support_tickets "
                   "WHERE party_source = 'nwv'").fetchone()[0] == 0
    assert ctx.sql("SELECT count(order_id) FROM raw.support_tickets "
                   "WHERE party_source = 'oms'").fetchone()[0] > 0


def test_the_acquired_leg_starts_when_the_book_landed(real_upstream):
    """The augmentation is dated, not sprinkled: Copperline had no Northwave
    tickets before the book arrived at E3."""
    lands = real_upstream.era("E3_northwave_acquisition")["book_lands"]
    assert real_upstream.sql(f"""
        SELECT count(*) FROM raw.support_tickets
        WHERE party_source = 'nwv' AND opened_at < DATE '{lands}'
    """).fetchone()[0] == 0


def test_the_crm_lands_no_files(small):
    """Halyard exports through a paginated API. The stub the ingest DAG calls
    is world source and belongs to the platform phase."""
    assert not (small.landing / "crm").exists()
    assert not (small.landing / "halyard").exists()


# --- the contract every module holds ----------------------------------------


def test_the_extracts_are_deterministic(tmp_path):
    """Two builds of the same config agree row for row. Nothing here reads a
    wall clock, and the test that greps for one lives in the sibling file."""
    prints = []
    for run in ("a", "b"):
        ctx = _context(tmp_path / run)
        for module in (clickstream, ads, pim, crm):
            module.build(ctx)
        prints.append({
            table: ctx.sql(f"SELECT count(*), coalesce(sum(hash(t)), 0) "
                           f"FROM {table} t").fetchone()
            for table in ("raw.web_events", "raw.ads_spend_daily",
                          "raw.email_events", "raw.pim_product_versions",
                          "raw.product_categories", "raw.customers",
                          "raw.nwv_accounts", "raw.support_tickets",
                          "ops.merge_candidates")
        })
    assert prints[0] == prints[1]
    assert all(count for count, _ in prints[0].values())


def test_nothing_lands_after_the_world_s_today(small):
    """The world is complete as of `today` and knows nothing later."""
    today = small.today
    for table, column in (("raw.web_events", "load_time"),
                          ("raw.ads_spend_daily", "loaded_at"),
                          ("raw.pim_product_versions", "received_at")):
        latest = small.sql(f"SELECT max({column}) FROM {table}").fetchone()[0]
        assert latest.date() < today, (table, latest)
