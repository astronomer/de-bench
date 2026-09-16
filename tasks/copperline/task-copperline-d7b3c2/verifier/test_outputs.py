"""GRO-455 — does the day's ad spend come out in dollars, at the right rate,
once, with the dollar campaigns still in it?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so `include.lib` resolves the warehouse the way every DAG does.

**Why a verifier rather than a DAG run.** `gro_channel_roi_daily` has never
finished a run — `worlds/copperline/BUILD-NOTES.md` has the three reasons and
none of them is this ticket's — so there is no DAG to replay. The build under
test is a module, and this calls it directly, three report dates and a rebuild,
which costs seconds rather than a dbt project.

**No authored numbers.** Every expected result is computed from
`raw.ads_spend_daily` and `raw.fx_rates` in this session, before anything is
built. The mart is stood up here because nothing in the workspace creates it.

**Both roundings pass.** `dbt/copperline_analytics/macros/money.sql` says
`to_base_cents` rounds half-up on a single division, and the expression it
actually ships lands in DOUBLE, where the same input can come out a cent
higher. A build that uses the macro and a build that does the integer
arithmetic the macro's comment describes therefore disagree by up to one cent
on about two rows in five. Neither is wrong, so every money comparison here
allows one cent. Every wrong answer this task is about — a dropped base
currency, a doubled delivery, a rate from the wrong day, parts per million read
as a multiplier — misses by thousands of cents or by an order of magnitude, so
the cent costs nothing.

**Three report dates, on purpose.**

    2026-04-16  inside the quarter the review reads
    2025-12-09  the previous fiscal year, and the day with the most
                restatements of the three
    2024-09-18  eighteen months before the ticket, so a fix scoped to recent
                dates fails here

The ticket names none of them. All three sit outside the world's reserved
windows and outside the never-grade tail.
"""

from __future__ import annotations

import duckdb
import pytest

from include.lib import warehouse

DB = str(warehouse.warehouse_path())

SPEND = warehouse.qualify("raw.ads_spend_daily")
RATES = warehouse.qualify("raw.fx_rates")
MARTS_SCHEMA = warehouse.qualify("marts")
TABLE = "marts.channel_spend_base_daily"
MART = warehouse.qualify(TABLE)

#: The report dates the build is measured on. See the module docstring.
GRADED = ("2026-04-16", "2025-12-09", "2024-09-18")

#: The reporting currency. `raw.fx_rates` publishes no row for it, which is the
#: whole of this ticket.
BASE = "USD"
BASE_PPM = 1_000_000

#: The published shape, as the ticket states it.
COLUMNS = (
    "ds", "platform", "campaign_id", "market_code", "channel", "currency_code",
    "fx_rate_ppm", "fx_rate_date", "spend_cents", "spend_base_cents",
    "attributed_revenue_cents", "attributed_revenue_base_cents",
)

DDL = f"""
CREATE TABLE IF NOT EXISTS {MART} (
    ds DATE,
    platform VARCHAR,
    campaign_id VARCHAR,
    market_code VARCHAR,
    channel VARCHAR,
    currency_code VARCHAR,
    fx_rate_ppm BIGINT,
    fx_rate_date DATE,
    spend_cents BIGINT,
    spend_base_cents BIGINT,
    attributed_revenue_cents BIGINT,
    attributed_revenue_base_cents BIGINT
)
"""

#: The expected answer, straight from the landed feed. One delivery per key,
#: the newest; the rate for the report date; USD at par because there is no row
#: to join to. `spend_halfup` is the integer arithmetic `money.sql` describes
#: and every money assertion allows a cent either side of it.
EXPECTED_SQL = f"""
WITH latest AS (
    SELECT platform, campaign_id, market_code, channel,
           arg_max(currency_code, loaded_at)            AS currency_code,
           arg_max(spend_cents, loaded_at)              AS spend_cents,
           arg_max(attributed_revenue_cents, loaded_at) AS revenue_cents
    FROM {SPEND}
    WHERE report_date = ?
    GROUP BY 1, 2, 3, 4
), rated AS (
    SELECT l.*,
           CASE WHEN l.currency_code = '{BASE}' THEN {BASE_PPM}
                ELSE r.rate_to_usd_ppm END              AS ppm
    FROM latest l
    LEFT JOIN {RATES} r
      ON r.currency_code = l.currency_code AND r.rate_date = ?
)
SELECT platform, campaign_id, market_code, channel, currency_code, ppm,
       spend_cents,
       CAST((CAST(spend_cents AS HUGEINT) * ppm + 500000) // 1000000 AS BIGINT),
       revenue_cents,
       CAST((CAST(revenue_cents AS HUGEINT) * ppm + 500000) // 1000000 AS BIGINT)
FROM rated
ORDER BY platform, campaign_id
"""

READ_SQL = f"SELECT {', '.join(COLUMNS)} FROM {MART}"


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def build(ds: str) -> None:
    """Run the patched build for one report date, the way the ticket asks for
    it to be callable."""
    try:
        from projects.growth.lib import spend
    except ImportError as exc:
        pytest.fail(f"FAIL: nothing to build with: projects/growth/lib/spend.py "
                    f"does not import ({exc})")
    entry = getattr(spend, "build_day", None)
    if not callable(entry):
        pytest.fail("FAIL: projects/growth/lib/spend.py has no build_day(ds)")
    entry(ds)


def row_count(ds: str) -> int:
    return query(f"SELECT count(*) FROM {MART} WHERE CAST(ds AS VARCHAR) = ?",
                 [ds])[0][0]


def expected_for(ds: str) -> dict:
    rows = query(EXPECTED_SQL, [ds, ds])
    return {
        (platform, campaign, market, channel): {
            "currency_code": currency,
            "fx_rate_ppm": ppm,
            "spend_cents": spend,
            "spend_base": spend_base,
            "revenue_cents": revenue,
            "revenue_base": revenue_base,
        }
        for (platform, campaign, market, channel, currency, ppm,
             spend, spend_base, revenue, revenue_base) in rows
    }


def published() -> dict:
    """Everything in the mart, keyed by report date and then by campaign key."""
    out: dict[str, dict] = {}
    try:
        rows = query(READ_SQL)
    except duckdb.Error as exc:
        pytest.fail(f"FAIL: {TABLE} does not hold the columns the ticket asks "
                    f"for ({exc})")
    for row in rows:
        record = dict(zip(COLUMNS, row))
        day = str(record["ds"])[:10]
        key = (record["platform"], record["campaign_id"],
               record["market_code"], record["channel"])
        out.setdefault(day, {})[key] = record
    return out


@pytest.fixture(scope="module")
def answers() -> dict:
    """Every expected result, taken from the feed before anything is built."""
    out = {}
    for ds in GRADED:
        want = expected_for(ds)
        assert want, f"the world lands no ad spend on {ds}, so nothing is graded"
        assert not [k for k, v in want.items() if v["fx_rate_ppm"] is None], (
            f"{ds}: a currency in the spend feed has no rate and is not "
            f"{BASE}; this task's expected answer is not defined for it"
        )
        out[ds] = want
    return out


@pytest.fixture(scope="module")
def built(answers) -> dict:
    """The mart, after the patched build has run each graded report date."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
        con.execute(DDL)
    finally:
        con.close()
    for ds in GRADED:
        build(ds)
    return published()


def rows_for(built: dict, ds: str) -> dict:
    rows = built.get(ds)
    assert rows, (
        f"FAIL: {ds}: the build published nothing for that report date; "
        f"{TABLE} holds {sorted(built) or 'no days at all'}"
    )
    return rows


def test_the_graded_days_can_tell_a_right_answer_from_a_wrong_one(answers):
    """The fixture's own guard, on three properties of the world this task
    depends on. If any of them stops being true the task stops grading what it
    says it grades, and this says so rather than passing quietly."""
    base_rows = query(f"SELECT count(*) FROM {RATES} WHERE currency_code = ?", [BASE])
    assert base_rows[0][0] == 0, (
        f"{RATES} now publishes a {BASE} row; the join this task grades no "
        "longer has anything to get wrong"
    )
    for ds in GRADED:
        delivered = query(f"SELECT count(*) FROM {SPEND} WHERE report_date = ?", [ds])[0][0]
        assert delivered > len(answers[ds]), (
            f"{ds}: {delivered} deliveries for {len(answers[ds])} campaigns — "
            "the day holds no restatement, so it cannot grade the resolution"
        )
        in_base = [v for v in answers[ds].values() if v["currency_code"] == BASE]
        assert in_base, f"{ds}: no campaign bills in {BASE}"
        assert len(in_base) < len(answers[ds]), f"{ds}: every campaign bills in {BASE}"


@pytest.mark.parametrize("ds", GRADED)
def test_a_report_date_publishes_one_row_for_every_campaign(built, answers, ds):
    """One figure per campaign per day. The feed holds every delivery of a
    report date; a build that adds them up publishes the same campaign twice or
    publishes one row that counts both."""
    rows = rows_for(built, ds)
    assert sorted(rows) == sorted(answers[ds]), (
        f"FAIL: {ds}: published {len(rows)} campaign rows against "
        f"{len(answers[ds])} campaigns in the feed"
    )
    assert row_count(ds) == len(answers[ds]), (
        f"FAIL: {ds}: {row_count(ds)} rows for {len(answers[ds])} campaigns — "
        "a campaign is published more than once"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_every_row_carries_the_report_dates_own_rate(built, answers, ds):
    """The rate that got the row there, and the date it is the rate for. A
    delivery that arrived a week late still converts at the report date, and
    the base currency's rate is 1,000,000 rather than nothing."""
    rows = rows_for(built, ds)
    for key, want in answers[ds].items():
        got = rows[key]
        assert got["fx_rate_ppm"] == want["fx_rate_ppm"], (
            f"FAIL: {ds} {key}: the row carries rate {got['fx_rate_ppm']} and "
            f"{want['currency_code']} is {want['fx_rate_ppm']} ppm on that date"
        )
        assert str(got["fx_rate_date"])[:10] == ds, (
            f"FAIL: {ds} {key}: the row is stamped with rate date "
            f"{got['fx_rate_date']}"
        )


@pytest.mark.parametrize("ds", GRADED)
def test_spend_lands_in_base_currency(built, answers, ds):
    """The figure the review adds up. One cent either way, for the reason the
    module docstring gives; everything this task is about misses by more."""
    rows = rows_for(built, ds)
    for key, want in answers[ds].items():
        got = rows[key]
        assert got["spend_cents"] == want["spend_cents"], (
            f"FAIL: {ds} {key}: published {got['spend_cents']} as billed and "
            f"the newest delivery says {want['spend_cents']}"
        )
        assert abs(got["spend_base_cents"] - want["spend_base"]) <= 1, (
            f"FAIL: {ds} {key}: {want['spend_cents']} {want['currency_code']} "
            f"at {want['fx_rate_ppm']} ppm is {want['spend_base']} base cents, "
            f"and the row says {got['spend_base_cents']}"
        )


@pytest.mark.parametrize("ds", GRADED)
def test_the_platforms_revenue_claim_converts_too(built, answers, ds):
    """Both money columns convert. A cost of sale worked from a converted spend
    and an unconverted return is further from the truth than the unconverted
    total the review has now."""
    rows = rows_for(built, ds)
    for key, want in answers[ds].items():
        got = rows[key]
        assert got["attributed_revenue_cents"] == want["revenue_cents"], (
            f"FAIL: {ds} {key}: published {got['attributed_revenue_cents']} as "
            f"claimed and the newest delivery says {want['revenue_cents']}"
        )
        assert abs(got["attributed_revenue_base_cents"] - want["revenue_base"]) <= 1, (
            f"FAIL: {ds} {key}: {want['revenue_cents']} {want['currency_code']} "
            f"at {want['fx_rate_ppm']} ppm is {want['revenue_base']} base "
            f"cents, and the row says {got['attributed_revenue_base_cents']}"
        )


@pytest.mark.parametrize("ds", GRADED)
def test_the_dollar_campaigns_are_still_in_the_day(built, answers, ds):
    """`raw.fx_rates` has no row for the base currency, and about half the
    day's spend is billed in it. A join that needs a rate row deletes those
    campaigns and the day still looks like money."""
    rows = rows_for(built, ds)
    want_base = {k: v for k, v in answers[ds].items() if v["currency_code"] == BASE}
    missing = sorted(k for k in want_base if k not in rows)
    assert not missing, (
        f"FAIL: {ds}: {len(missing)} campaigns billed in {BASE} are not in the "
        f"day at all, starting with {missing[0]}"
    )
    for key, want in want_base.items():
        assert abs(rows[key]["spend_base_cents"] - want["spend_cents"]) <= 1, (
            f"FAIL: {ds} {key}: billed {want['spend_cents']} {BASE} cents and "
            f"published {rows[key]['spend_base_cents']} base cents"
        )


def test_a_rebuilt_report_date_replaces_itself_and_leaves_the_others(built):
    """A restatement makes a closed report date pending again, so the build is
    run twice on the same date as a matter of course. The second run replaces
    that date and touches no other."""
    again, others = GRADED[0], GRADED[1:]
    before = {ds: rows_for(built, ds) for ds in GRADED}
    counts = {ds: row_count(ds) for ds in GRADED}
    build(again)
    after = published()
    for ds in (again, *others):
        assert after.get(ds) == before[ds] and row_count(ds) == counts[ds], (
            f"FAIL: rebuilding {again} left {ds} holding {row_count(ds)} rows "
            f"where it held {counts[ds]}"
        )


def test_the_warehouse_read_is_the_scored_tree():
    """A guard on the verifier itself."""
    import os

    assert os.path.exists(DB)
