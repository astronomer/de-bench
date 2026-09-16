"""The S2 payment extracts: Meridian, Halcyon, the marketplace, and the rate
table every conversion reads.

These build small Contexts directly rather than going through the CLI — the
whole simulated upstream plus the four extract modules is about fifteen
seconds, where an end-to-end CLI run is about eighty.

What the file is really asserting is the boundary between two claims. Spec 03
chapter 7 states populations; the simulation states what happened. Where they
disagree the integrator ruled for the simulation, and the assertions below
are written against what the world actually holds, with the chapter's number
beside it wherever the two differ.
"""

import datetime as dt
import json
import re
import sys
from pathlib import Path

import duckdb
import pytest

from de_bench.tasks import repo_root

sys.path.insert(0, str(repo_root() / "tools"))

from gen_copperline import config, upstream                       # noqa: E402
from gen_copperline.extracts import (                             # noqa: E402
    calendars, fx, halcyon, marketplace, meridian)
from gen_copperline.upstream import _util                         # noqa: E402

WORLD = repo_root() / "worlds" / "copperline"

# The overlap quarter, from timeline.yaml E5.
E5_FROM, E5_TO = "2025-07-01", "2025-09-30"

# Chapter populations at the shipped profile, and the small profile divides
# fact volumes by config.SMALL_DIVISOR.
CHAPTER_BOTH_FEEDS = 193_000
CHAPTER_SOFT_DELETE_RATE = 4_800 / 1_600_000       # 0.30% of landed events


def _build(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    """The whole upstream, then the four extracts, at the small profile."""
    landing = tmp_path / "landing"
    landing.mkdir(parents=True, exist_ok=True)
    cfg = config.load_timeline(WORLD / "timeline.yaml")
    con = duckdb.connect(str(tmp_path / "copperline.duckdb"))
    ctx = config.Context(con=con, cfg=cfg, profile="small", landing=landing)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    _util.ensure_days(ctx)
    for module in upstream.ORDER:
        module.build(ctx)
    for module in (calendars, fx, meridian, halcyon, marketplace):
        module.build(ctx)
    return con


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    root = tmp_path_factory.mktemp("x2")
    con = _build(root)
    yield con, root / "landing"
    con.close()


@pytest.fixture(scope="module")
def con(built):
    return built[0]


@pytest.fixture(scope="module")
def landing(built):
    return built[1]


def _one(con, sql):
    return con.execute(sql).fetchone()


def _scaled(n: float) -> float:
    return n / config.SMALL_DIVISOR


# --- E5: the quarter both processors fed ------------------------------------

def test_the_overlap_quarter_carries_both_feeds_for_the_same_payments(con):
    """Spec 03 section 7: about 193,000 payments are in both feeds during the
    migration quarter, so a union that does not read the window table
    double-counts roughly a quarter of the year's settlement volume.

    The two landed feeds share no key — that gap is the reconciliation seam —
    so the count is taken through the simulation's own `gateway_reference`,
    which is what both were derived from."""
    both = _one(con, f"""
        SELECT count(*) FROM (
            SELECT DISTINCT gateway_reference FROM sim_ext.halcyon_settlements
            WHERE txn_local_ts::DATE BETWEEN DATE '{E5_FROM}' AND DATE '{E5_TO}'
        ) JOIN (
            SELECT DISTINCT gateway_reference FROM sim_ext.meridian_events
        ) USING (gateway_reference)""")[0]
    want = _scaled(CHAPTER_BOTH_FEEDS)
    assert 0.9 * want <= both <= 1.1 * want, both

    # Both feeds really are carrying the quarter, so the overlap is a genuine
    # double count and not one feed's rows appearing twice.
    halcyon_q, meridian_q = _one(con, f"""
        SELECT (SELECT count(*) FROM raw.pay_halcyon_settlements
                WHERE txn_datetime >= '{E5_FROM}' AND txn_datetime < '2025-10-01'),
               (SELECT count(*) FROM raw.pay_meridian_settlements
                WHERE event_time_utc::DATE BETWEEN DATE '{E5_FROM}' AND DATE '{E5_TO}')""")
    assert halcyon_q > both * 0.9
    assert meridian_q > both


def test_meridian_says_nothing_about_anything_before_the_overlap(con):
    """Meridian goes live at the start of the quarter. A settlement before it
    would give a naive union nothing to disagree about."""
    assert _one(con, f"SELECT count(*) FROM raw.pay_meridian_settlements "
                     f"WHERE event_time_utc < TIMESTAMP '{E5_FROM}'") == (0,)
    assert _one(con, f"SELECT count(*) FROM raw.payment_intents "
                     f"WHERE created_at < TIMESTAMP '{E5_FROM}'") == (0,)

    # And Halcyon stops at the end of it, so after the cutover Meridian is
    # alone. The last transaction is inside the quarter; its settlement and
    # its file follow a few days later, which is the feed's own lag.
    last_txn, last_file = _one(con, "SELECT max(txn_datetime), max(file_date) "
                                    "FROM raw.pay_halcyon_settlements")
    assert last_txn[:10] <= E5_TO
    assert E5_TO < str(last_file) <= "2025-10-05"


def test_nothing_lands_after_today(con):
    """The landing layer holds what has been delivered. The simulation carries
    a refund as far as 46 days past its capture, which at the end of the range
    is a delivery that has not happened."""
    today = str(dt.date.fromisoformat(
        str(config.load_timeline(WORLD / "timeline.yaml")["today"])))
    for table, column in (("pay_meridian_settlements", "loaded_at"),
                          ("marketplace_orders", "loaded_at"),
                          ("marketplace_settlements", "loaded_at")):
        assert _one(con, f"SELECT count(*) FROM raw.{table} "
                         f"WHERE {column} >= TIMESTAMP '{today}'") == (0,), table
    assert _one(con, f"SELECT count(*) FROM raw.pay_halcyon_settlements "
                     f"WHERE file_date >= DATE '{today}'") == (0,)


# --- the ambiguous reference and the bridge that is not ---------------------

def test_the_processor_reference_recycles_every_fifty_days(con):
    """`OE-#######` holds ten million and the order key block is 200,000 a
    day, so two orders share a reference 50 days apart. That is a property of
    a seven-digit reference, not a fault — what makes the world coherent is
    that the bridge carries `order_id` beside it."""
    assert meridian.collision_cadence_days() == 50

    n_ref, n_order = _one(con, "SELECT count(DISTINCT order_ref), "
                               "count(DISTINCT order_id) FROM raw.payment_intents")
    assert n_ref < n_order, "the reference has to be ambiguous"

    # Two orders sharing one reference are a whole number of cadences apart.
    cadence = meridian.collision_cadence_days()
    off_cadence = _one(con, f"""
        SELECT count(*) FROM (
            SELECT order_ref, min(order_id) AS lo, max(order_id) AS hi
            FROM raw.payment_intents GROUP BY order_ref HAVING count(DISTINCT order_id) > 1)
        WHERE (hi - lo) % ({cadence} * {_util.DAY_BLOCK}) <> 0""")[0]
    assert off_cadence == 0

    # And the bridge is not ambiguous. Its own key is dense and unique, and
    # `order_id` answers the question the reference cannot.
    n_rows, n_intents = _one(con, "SELECT count(*), count(DISTINCT intent_id) "
                                  "FROM raw.payment_intents")
    assert n_rows == n_intents
    unresolved = _one(con, """
        SELECT count(*) FROM raw.pay_meridian_settlements s
        LEFT JOIN raw.payment_intents i USING (intent_id)
        WHERE i.intent_id IS NULL""")[0]
    assert unresolved == 0, "every settlement resolves to exactly one order"


def test_the_event_id_is_the_idempotency_key(con):
    """Spec 03 section 21: `event_id` is the true idempotency key, so it is
    the one id in the feed that must never repeat."""
    n, distinct = _one(con, "SELECT count(*), count(DISTINCT event_id) "
                            "FROM raw.pay_meridian_settlements")
    assert n == distinct


# --- the restatement populations -------------------------------------------

def test_the_restatement_boundary_populations_are_exact(con):
    """Twenty rows at exactly 30 days back and twenty at 31 — the two sides of
    R-5 — and 120 that restate a closed fiscal month, which is REV-8's
    adjustment-row branch. Exact at every profile: a graded boundary that
    scaled away would stop being graded."""
    back = dict(con.execute("""
        SELECT date_diff('day', b.event_time_utc, r.event_time_utc), count(*)
        FROM raw.pay_meridian_settlements r
        JOIN raw.pay_meridian_settlements b ON b.event_id = r.restates_event_id
        GROUP BY 1 ORDER BY 1""").fetchall())
    assert back[30] == meridian.BOUNDARY_30_ROWS
    assert back[31] == meridian.BOUNDARY_31_ROWS
    assert max(back) == 31, "nothing reaches past the R-5 boundary"

    closed = _one(con, """
        SELECT count(*) FROM raw.pay_meridian_settlements r
        JOIN raw.pay_meridian_settlements b ON b.event_id = r.restates_event_id
        JOIN (SELECT cal_date, max(cal_date) OVER (
                         PARTITION BY fiscal_year, fiscal_period) AS period_end
              FROM raw.fiscal_calendar) p ON p.cal_date = b.event_time_utc::DATE
        WHERE r.event_time_utc::DATE > p.period_end + 7""")[0]
    assert closed == meridian.CLOSED_MONTH_ROWS

    # Everything else stays inside the restated event's own period, which is
    # what keeps the closed-month count exactly 120 and not 120-and-a-tail.
    bulk = sum(n for d, n in back.items() if d <= meridian.BULK_LAG_HI)
    assert bulk == sum(back.values()) - meridian.CLOSED_MONTH_ROWS


def test_the_soft_delete_population_sits_at_the_chapter_rate(con):
    """4,800 soft deletes in 1,600,000 events, which is 0.3%."""
    deleted, total = _one(con, """
        SELECT count(*) FILTER (WHERE deleted_at IS NOT NULL), count(*)
        FROM raw.pay_meridian_settlements""")
    rate = deleted / total
    assert 0.7 * CHAPTER_SOFT_DELETE_RATE <= rate <= 1.3 * CHAPTER_SOFT_DELETE_RATE, rate

    # A soft-deleted row is still a row. Nothing here removes it.
    assert _one(con, "SELECT count(*) FROM raw.pay_meridian_settlements "
                     "WHERE deleted_at IS NOT NULL AND deleted_at < event_time_utc") == (0,)


def test_the_restatement_population_follows_the_simulation(con):
    """The chapter asks for 29,000 restatements in 1,600,000 events — 1.8%.
    `upstream.external` marks 1.8% of *payments* rather than of events, which
    lands at about 0.9% of the feed. Membership is the simulation's and only
    the timing is set here, so this asserts what the world holds. Raising it
    is one number in `upstream.external._meridian`: the `e.n = 3` window."""
    restated, total = _one(con, """
        SELECT count(*) FILTER (WHERE restates_event_id IS NOT NULL), count(*)
        FROM raw.pay_meridian_settlements""")
    assert 0.006 <= restated / total <= 0.012, restated / total
    assert restated > meridian.CLOSED_MONTH_ROWS


# --- Halcyon: the strings, the missing currency, the missing zone -----------

_TWO_DECIMAL = re.compile(r"^-?\d+\.\d\d$")


def test_halcyon_amounts_are_two_decimal_strings_that_parse_exactly(con):
    """The one float-shaped money column in the world. It is written by
    casting the decimal, so every value round-trips to the exact cent."""
    dtype = _one(con, "SELECT data_type FROM information_schema.columns "
                      "WHERE table_schema = 'raw' "
                      "AND table_name = 'pay_halcyon_settlements' "
                      "AND column_name = 'amount'")[0]
    assert dtype == "VARCHAR"

    rows = con.execute("SELECT amount FROM raw.pay_halcyon_settlements "
                       "ORDER BY txn_id LIMIT 5000").fetchall()
    assert all(_TWO_DECIMAL.match(a) for (a,) in rows)

    # The text is canonical: parsing it and printing it again is the same
    # string, so nothing was rounded or widened on the way out.
    assert _one(con, "SELECT count(*) FROM raw.pay_halcyon_settlements "
                     "WHERE amount::DECIMAL(18,2)::VARCHAR <> amount") == (0,)

    # And the feed carries the money the simulation settled, to the cent.
    landed, settled = _one(con, """
        SELECT (SELECT sum(round(amount::DECIMAL(18,2) * 100)::BIGINT)
                FROM raw.pay_halcyon_settlements),
               (SELECT sum(round(settled_amount * 100)::BIGINT)
                FROM sim_ext.halcyon_settlements)""")
    assert landed == settled

    # No float anywhere else in the feed.
    floats = con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = 'pay_halcyon_settlements'
          AND data_type IN ('FLOAT', 'DOUBLE')""").fetchall()
    assert floats == []


def test_the_missing_currency_is_a_third_of_the_feed_and_always_recoverable(con):
    """NULL on 34% of rows. The merchant account has decided the currency
    since before the column existed, so `raw.merchant_regions` is the way
    home — the column is a trap after E4 and furniture before it."""
    rate = _one(con, """
        SELECT count(*) FILTER (WHERE currency IS NULL) * 1.0 / count(*)
        FROM raw.pay_halcyon_settlements""")[0]
    assert 0.31 <= rate <= 0.37, rate

    # Every account joins, and each one bills in exactly one currency, so a
    # NULL never costs a row its currency.
    assert _one(con, """
        SELECT count(*) FROM raw.pay_halcyon_settlements h
        LEFT JOIN raw.merchant_regions m USING (merchant_acct)
        WHERE m.merchant_acct IS NULL""") == (0,)
    ambiguous = _one(con, """
        SELECT count(*) FROM (
            SELECT merchant_acct FROM raw.pay_halcyon_settlements
            WHERE currency IS NOT NULL
            GROUP BY merchant_acct HAVING count(DISTINCT currency) > 1)""")[0]
    assert ambiguous == 0

    # Before E4 the NULL is harmless: every settlement was USD anyway.
    e4 = str(config.load_timeline(WORLD / "timeline.yaml")
             ["eras"]["E4_halcyon_non_usd"]["at"])
    assert _one(con, f"SELECT count(*) FROM raw.pay_halcyon_settlements "
                     f"WHERE txn_datetime < '{e4}' AND currency IS NOT NULL "
                     f"AND currency <> 'USD'") == (0,)
    non_usd = _one(con, f"""
        SELECT count(*) FILTER (WHERE m.region_code NOT IN ('US', 'CA')) * 1.0 / count(*)
        FROM raw.pay_halcyon_settlements h
        JOIN raw.merchant_regions m USING (merchant_acct)
        WHERE h.txn_datetime >= '{e4}'""")[0]
    assert 0.25 <= non_usd <= 0.40, non_usd


def test_halcyon_time_has_no_zone_and_never_gets_one(con):
    """Merchant-local text with no offset. The feed stops before E6, so the
    UTC column it never had is a column it never gets."""
    cols = dict(con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = 'pay_halcyon_settlements'"
    ).fetchall())
    assert cols["txn_datetime"] == "VARCHAR"
    assert not [c for c in cols if c.endswith("_utc")]
    assert _one(con, "SELECT count(*) FROM raw.pay_halcyon_settlements "
                     "WHERE txn_datetime NOT SIMILAR TO "
                     r"'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'") == (0,)


# --- the marketplace: whose money is whose ---------------------------------

def test_the_principal_signs_negative_and_gmv_is_never_revenue(con):
    """The feed is written from the operator's side, so the principal is money
    out. GMV is the seller's number (REV-14)."""
    signs = dict(con.execute("""
        SELECT line_type, count(*) FILTER (WHERE amount_cents < 0)
             = count(*) FROM raw.marketplace_settlements GROUP BY 1""").fetchall())
    assert signs["principal"] is True
    assert signs["commission"] is False
    assert signs["fulfilment_fee"] is False

    # Copperline's take is the commission and the fee, an order of magnitude
    # under GMV. Anything that reads GMV as revenue is out by that much.
    gmv, take = _one(con, "SELECT sum(gmv_cents), "
                          "sum(commission_cents) + sum(fulfilment_fee_cents) "
                          "FROM raw.marketplace_orders")
    assert take < gmv / 5

    # A payout ties to the lines it was rolled up from.
    assert _one(con, """
        SELECT count(*) FROM (
            SELECT payout_id, -sum(amount_cents) AS net
            FROM raw.marketplace_settlements GROUP BY payout_id) l
        JOIN raw.marketplace_payouts p USING (payout_id)
        WHERE p.net_paid_cents <> l.net""") == (0,)


def test_the_recognition_date_is_the_ship_confirmation(con):
    """REV-14 turns on `ship_confirmed_at`, which is one to six days after the
    order, so a month end sits between the two on a real share of rows."""
    assert _one(con, "SELECT count(*) FROM raw.marketplace_orders "
                     "WHERE ship_confirmed_at <= placed_at") == (0,)
    n_cross, n_all = _one(con, """
        SELECT count(*) FILTER (WHERE date_trunc('month', ship_confirmed_at)
                                  <> date_trunc('month', placed_at)),
               count(*) FROM raw.marketplace_orders""")
    assert 0.02 <= n_cross / n_all <= 0.20, n_cross / n_all

    # The marketplace tail is the thickest in the world: 71% same day.
    same_day = _one(con, """
        SELECT count(*) FILTER (WHERE loaded_at::DATE = placed_at::DATE) * 1.0
             / count(*) FROM raw.marketplace_orders""")[0]
    assert 0.65 <= same_day <= 0.77, same_day


# --- the rate table ---------------------------------------------------------

def test_fx_is_daily_integer_parts_per_million(con):
    """Spec 03 section 15: one row per date per currency, 6,034 of them, and
    the rate as an integer so the arithmetic is reproducible."""
    n, currencies, days = _one(con, "SELECT count(*), count(DISTINCT currency_code), "
                                    "count(DISTINCT rate_date) FROM raw.fx_rates")
    assert (n, currencies) == (6034, 7)
    assert n == currencies * days

    assert _one(con, "SELECT count(*) FROM raw.fx_rates "
                     "WHERE currency_code = 'USD'") == (0,)
    dtype = _one(con, "SELECT data_type FROM information_schema.columns "
                      "WHERE table_schema = 'raw' AND table_name = 'fx_rates' "
                      "AND column_name = 'rate_to_usd_ppm'")[0]
    assert dtype == "BIGINT"

    # The rate is published the morning after the date it is for.
    assert _one(con, "SELECT count(*) FROM raw.fx_rates "
                     "WHERE published_at::DATE <> rate_date + 1") == (0,)

    # Indonesia is why the unit is parts per million and not two decimals.
    idr = _one(con, "SELECT min(rate_to_usd_ppm), max(rate_to_usd_ppm) "
                    "FROM raw.fx_rates WHERE currency_code = 'IDR'")
    assert 0 < idr[0] <= idr[1] < 1000


# --- the landing tree -------------------------------------------------------

def test_the_meridian_landing_holds_ninety_hourly_days(con, landing):
    """`landing/meridian/dt=<ds>/hr=<hh>/events.jsonl`, the 90 live days RET-1
    keeps. Anything older is the archive's problem, which is DR-1's."""
    days = sorted(p.name for p in (landing / "meridian").glob("dt=*"))
    assert len(days) == meridian.RETENTION_DAYS
    assert days[-1] == "dt=2026-06-14"

    files = list((landing / "meridian").glob("dt=*/hr=*/events.jsonl"))
    assert len(files) > len(days), "the hour partition has to carry more than one hour"
    assert len({p.parent.name for p in files}) == 24


def test_one_landing_hour_matches_the_table_row_for_row(con, landing):
    """A rebuild from the files has to reproduce the table, not approximate
    it, so `loaded_at` is written into the row and not only into the path."""
    one = sorted((landing / "meridian").glob("dt=*/hr=*/events.jsonl"))[45]
    day = one.parent.parent.name.removeprefix("dt=")
    hour = one.parent.name.removeprefix("hr=")

    rows = [json.loads(line) for line in one.read_text().splitlines()]
    assert rows
    assert set(rows[0]) == {
        "event_id", "payment_id", "intent_id", "order_ref", "processor_txn_id",
        "event_type", "amount_cents", "currency_code", "event_time_utc",
        "loaded_at", "settlement_date", "restates_event_id", "attempt_no",
        "deleted_at"}
    assert all(isinstance(r["amount_cents"], int) for r in rows)
    assert all(r["loaded_at"].startswith(f"{day}T{hour}:") for r in rows)

    n_table = _one(con, f"""
        SELECT count(*) FROM raw.pay_meridian_settlements
        WHERE loaded_at::DATE = DATE '{day}'
          AND date_part('hour', loaded_at) = {int(hour)}""")[0]
    assert len(rows) == n_table

    ids = {r["event_id"] for r in rows}
    assert len(ids) == len(rows)
    in_table = _one(con, f"""
        SELECT count(*) FROM raw.pay_meridian_settlements
        WHERE event_id IN ({', '.join(repr(i) for i in sorted(ids))})""")[0]
    assert in_table == len(rows)


def test_the_halcyon_landing_is_one_csv_per_account_per_night(con, landing):
    """`landing/halcyon/dt=<ds>/settlement_<acct>.csv` over the overlap
    quarter — the window PAY-207 reads and the last one the feed wrote."""
    days = sorted(p.name for p in (landing / "halcyon").glob("dt=*"))
    assert days[0] == f"dt={E5_FROM}"
    accounts = {p.name for p in (landing / "halcyon").glob("dt=*/*.csv")}
    assert len(accounts) == 6
    assert all(a.startswith("settlement_HAL-") and a.endswith(".csv")
               for a in accounts)

    one = sorted((landing / "halcyon").glob(f"dt={E5_FROM}/*.csv"))[0]
    lines = one.read_text().splitlines()
    assert lines[0] == ("txn_id,merchant_ref,status,amount,currency,txn_datetime,"
                        "settled_on,file_date,batch_id,merchant_acct")
    acct = one.name.removeprefix("settlement_").removesuffix(".csv")
    n_table = _one(con, f"SELECT count(*) FROM raw.pay_halcyon_settlements "
                        f"WHERE file_date = DATE '{E5_FROM}' "
                        f"AND merchant_acct = '{acct}'")[0]
    assert len(lines) - 1 == n_table


def test_no_staging_directory_survives_the_build(landing):
    """The partitioned write goes through a staging tree. If one is left
    behind the world ships a directory nobody named."""
    assert sorted(p.name for p in landing.iterdir()) == ["halcyon", "meridian"]


# --- determinism ------------------------------------------------------------

_FINGERPRINTED = (
    "raw.fx_rates", "raw.pay_meridian_settlements", "raw.payment_intents",
    "raw.pay_halcyon_settlements", "raw.merchant_regions",
    "raw.marketplace_orders", "raw.marketplace_settlements",
    "raw.marketplace_payouts",
)


def _fingerprint(con) -> dict:
    return {t: _one(con, f"SELECT count(*), coalesce(sum(hash(x)), 0) "
                         f"FROM {t} x")
            for t in _FINGERPRINTED}


def test_two_builds_of_one_seed_are_the_same_world(built, tmp_path_factory):
    """No wall clock, no unseeded draw, no ordering that depends on how
    DuckDB felt. A second build has to be the first one."""
    first, first_landing = built
    second_root = tmp_path_factory.mktemp("x2_again")
    second = _build(second_root)
    try:
        assert _fingerprint(second) == _fingerprint(first)

        a = sorted((first_landing / "meridian").glob("dt=*/hr=*/events.jsonl"))[45]
        b = (second_root / "landing" / "meridian" / a.parent.parent.name
             / a.parent.name / a.name)
        assert b.read_text() == a.read_text()
    finally:
        second.close()
