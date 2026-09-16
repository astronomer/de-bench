"""S7 — the marketing platforms: `raw.ads_spend_daily`, `raw.email_events`,
and the dated CSVs under `landing/ads/`.

Beacon Ads, Tessera Social and Solstice Search report spend. Larkspur is the
e-mail platform and reports events, never a spend row, which is why it holds
campaigns in `sim_ext` and never appears in `ads_spend_daily`.

## The restatement, which is the whole point of this source

An ad platform delivers a day's numbers the morning after, and then delivers
them **again** three to seven days later with different numbers, because
late conversions and invalid-click credits land after the fact. Spec 03
section 12 arms `restated_at` for exactly this: a freshness alarm on this
table has an honest reason to look wrong.

So `raw.ads_spend_daily` is **not** one row per (report date, platform,
campaign). It is one row per *delivery* of that key, and it holds every
delivery it ever received:

  * the first delivery carries `loaded_at` at 04:00 the next morning and a
    NULL `restated_at`;
  * a restatement carries a later `loaded_at`, the same `restated_at`, and
    moved `impressions`, `clicks`, `spend_cents` and attribution.

Reading the table without taking the latest delivery per key double-counts
spend on about one key in eleven. That question is real because the earlier
delivery is still there, which is what a landing zone looks like.

The landing files make the same thing visible from the other side: the file
for a date holds what was delivered on that date, so a restated (platform,
report_date) turns up in a later day's file with moved numbers next to its
original.

## Money and currency

Integer cents, per spec 03 section 1 rule 1 — not micro-units. The platforms
bill in the market's own currency, so `currency_code` sits on the row and
there is no `fx_rate_ppm`: the FX join through `raw.fx_rates` is the work
the column arms.

## Delivery

`landing/ads/dt=<ds>/<platform>.csv`, one file per platform per delivery
date, for the 90 days RET-1 keeps a vendor file live. `larkspur.csv` holds
that night's bulk e-mail export rather than a spend report.
"""

from __future__ import annotations

from ..config import Context
from ..streams import draw
from . import _land

SPEND_PLATFORMS = ("beacon", "tessera", "solstice")
EMAIL_PLATFORM = "larkspur"

# The first delivery lands at 04:00 the next morning.
FIRST_DELIVERY_HOUR = 4

# A restatement moves the numbers. Spend and attribution drift up more often
# than down: late conversions arrive after the invalid-click credits do.
SPEND_DRIFT_PCT = (-6, 14)
IMPRESSION_DRIFT_PCT = (-2, 5)
ATTRIBUTION_DRIFT_PCT = (-4, 26)


def _percent(h: str, lo: int, hi: int) -> str:
    """A multiplier in [1 + lo/100, 1 + hi/100], as integer arithmetic."""
    return f"(100 + {lo} + ({h}) % {hi - lo + 1})"


def _spend(ctx: Context) -> None:
    """`raw.ads_spend_daily` — every delivery, with the clock it arrived on."""
    s = ctx.seed
    h_spend = draw(s, "'ads_drift_spend'", "a.report_date", "a.campaign_id")
    h_impr = draw(s, "'ads_drift_impr'", "a.report_date", "a.campaign_id")
    h_attr = draw(s, "'ads_drift_attr'", "a.report_date", "a.campaign_id")
    platforms = ", ".join(f"'{p}'" for p in SPEND_PLATFORMS)

    ctx.sql(f"""
        CREATE OR REPLACE TABLE raw.ads_spend_daily AS
        WITH src AS (
            SELECT a.*,
                   {_percent(h_spend, *SPEND_DRIFT_PCT)}  AS spend_pct,
                   {_percent(h_impr, *IMPRESSION_DRIFT_PCT)} AS impr_pct,
                   {_percent(h_attr, *ATTRIBUTION_DRIFT_PCT)} AS attr_pct
            FROM sim_ext.ads_spend_daily a
            WHERE a.platform IN ({platforms})
        ), first_delivery AS (
            SELECT report_date, platform, campaign_id, campaign_name,
                   market_code, channel,
                   impressions::BIGINT                            AS impressions,
                   clicks::BIGINT                                 AS clicks,
                   round(spend_amount * 100)::BIGINT              AS spend_cents,
                   currency_code,
                   attributed_orders::BIGINT                      AS attributed_orders,
                   round(attributed_revenue * 100)::BIGINT
                                                       AS attributed_revenue_cents,
                   (report_date + INTERVAL 1 DAY)::TIMESTAMP
                       + INTERVAL {FIRST_DELIVERY_HOUR} HOUR      AS loaded_at,
                   NULL::TIMESTAMP                                AS restated_at
            FROM src
        ), restatement AS (
            SELECT report_date, platform, campaign_id, campaign_name,
                   market_code, channel,
                   (impressions * impr_pct // 100)::BIGINT        AS impressions,
                   (clicks * impr_pct // 100)::BIGINT             AS clicks,
                   (round(spend_amount * 100) * spend_pct // 100)::BIGINT
                                                                  AS spend_cents,
                   currency_code,
                   (attributed_orders * attr_pct // 100)::BIGINT  AS attributed_orders,
                   (round(attributed_revenue * 100) * attr_pct // 100)::BIGINT
                                                       AS attributed_revenue_cents,
                   restated_at                                    AS loaded_at,
                   restated_at
            FROM src
            WHERE restated_at IS NOT NULL
        )
        SELECT * FROM (
            SELECT * FROM first_delivery
            UNION ALL
            SELECT * FROM restatement
        )
        -- The world is complete as of `today`. The last report has not been
        -- delivered yet, and the restatements behind it have not been sent.
        WHERE loaded_at < DATE '{ctx.today}'
    """)


def _email(ctx: Context) -> None:
    """`raw.email_events` — Larkspur's nightly bulk export.

    `email_hash` is armed for PII-1: it is derived from the address, which is
    a tagged column wherever it is held, so a governance rule that only looks
    at columns named `email` misses it. The address itself never lands here,
    which is why the platform is allowed to send the hash at all. When the
    customer module builds, the address behind the hash should come from
    `sim_customer.customers.email` rather than from the deterministic one
    below; the hash is opaque either way and no expected result reads it.
    """
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.email_events AS
        SELECT
            e.send_id,
            e.campaign_id,
            'C-' || lpad(e.customer_ref::VARCHAR, 6, '0')      AS customer_ref,
            md5('user' || e.customer_ref::VARCHAR || '@copperline-mail.example')
                                                              AS email_hash,
            e.event_type,
            e.event_time_utc,
            e.message_id
        FROM sim_ext.email_events e
    """)


def _land_files(ctx: Context) -> None:
    """One CSV per platform per delivery date, over RET-1's live window."""
    start = _land.window_start(ctx, _land.VENDOR_DAYS)
    for platform in SPEND_PLATFORMS:
        _land.write_hive(
            ctx,
            f"""
            SELECT loaded_at::DATE AS dt,
                   report_date, platform, campaign_id, campaign_name,
                   market_code, channel, impressions, clicks, spend_cents,
                   currency_code, attributed_orders, attributed_revenue_cents,
                   loaded_at, restated_at
            FROM raw.ads_spend_daily
            WHERE platform = '{platform}' AND loaded_at >= DATE '{start}'
            ORDER BY report_date, campaign_id, loaded_at
            """,
            ctx.landing / "ads", "CSV", ("dt",), platform, "HEADER")
    _land.write_hive(
        ctx,
        f"""
        SELECT event_time_utc::DATE AS dt,
               send_id, campaign_id, customer_ref, email_hash, event_type,
               event_time_utc, message_id
        FROM raw.email_events
        WHERE event_time_utc >= DATE '{start}'
        ORDER BY event_time_utc, send_id
        """,
        ctx.landing / "ads", "CSV", ("dt",), EMAIL_PLATFORM, "HEADER")


def _check(ctx: Context) -> None:
    keys, deliveries, restated = ctx.sql("""
        SELECT count(DISTINCT (report_date, platform, campaign_id)),
               count(*),
               count(*) FILTER (WHERE restated_at IS NOT NULL)
        FROM raw.ads_spend_daily
    """).fetchone()
    assert deliveries == keys + restated, (deliveries, keys, restated)
    assert restated, "no (platform, date) was ever restated"

    # A restatement arrives three to seven days after the report date, and
    # always after the delivery it restates.
    off = ctx.sql(f"""
        SELECT count(*) FROM raw.ads_spend_daily
        WHERE restated_at IS NOT NULL
          AND (datediff('day', report_date, restated_at::DATE) NOT BETWEEN 3 AND 7
               OR loaded_at <= (report_date + INTERVAL 1 DAY)::TIMESTAMP
                               + INTERVAL {FIRST_DELIVERY_HOUR} HOUR)
    """).fetchone()[0]
    assert not off, f"{off} restatement(s) outside the three-to-seven-day window"

    # Larkspur reports events, never spend.
    stray = ctx.sql("SELECT count(*) FROM raw.ads_spend_daily "
                    f"WHERE platform = '{EMAIL_PLATFORM}'").fetchone()[0]
    assert not stray, f"{stray} spend row(s) from the e-mail platform"


def build(ctx: Context) -> None:
    _spend(ctx)
    _email(ctx)
    _check(ctx)
    _land_files(ctx)
