"""The ad platforms' spend feed, and what a delivery is.

Beacon Ads, Tessera Social and Solstice Search each drop one CSV a morning at
`landing/ads/dt=<ds>/<platform>.csv`. The file named for a day holds what
that platform DELIVERED that day, which is not the same as what happened that
day: the first delivery of a report date lands at about 04:00 the next
morning, and the same report date is delivered AGAIN three to seven days
later with different numbers, because late conversions and invalid-click
credits arrive after the fact.

So `raw.ads_spend_daily` is one row per delivery, not one row per
(report date, platform, campaign). Both deliveries stay: the first with a
NULL `restated_at`, the restatement with the same `restated_at` on it and a
later `loaded_at`. That is what a landing table looks like and it is what
lets anybody ask what we believed on a given morning.

**The load is scoped by platform and delivery date together.** One partition
column cannot say both, which is why this module holds the write rather than
`CsvToWarehouseOperator`: a delete keyed on the delivery date alone would
take the other two platforms' rows out on the way past.

Money is integer cents and the platforms bill in the market's own currency,
so `currency_code` sits on the row and there is no rate. Converting is a join
to `raw.fx_rates` at the report date, and it belongs to whoever is reporting,
not to the intake.
"""

from __future__ import annotations

import datetime as dt

from include.lib import landing_dir, warehouse

__all__ = ["SPEND_PLATFORMS", "EMAIL_PLATFORM", "SPEND_COLUMNS", "delivery_file",
           "land_delivery", "land_email", "restated_keys", "missing_rates",
           "report_dates_pending", "spend_ties"]

#: The three platforms that report spend. Larkspur is e-mail and reports
#: events, never a spend row.
SPEND_PLATFORMS = ("beacon", "tessera", "solstice")
EMAIL_PLATFORM = "larkspur"

#: The delivery grain, in the order the platforms write it.
SPEND_COLUMNS = (
    "report_date", "platform", "campaign_id", "campaign_name", "market_code",
    "channel", "impressions", "clicks", "spend_cents", "currency_code",
    "attributed_orders", "attributed_revenue_cents", "loaded_at", "restated_at",
)


def delivery_file(platform: str, ds: str | dt.date) -> str:
    """Where a platform's delivery for a date lands."""
    return str(landing_dir("ads") / f"dt={_as_date(ds)}" / f"{platform}.csv")


def land_delivery(platform: str, ds: str | dt.date) -> int:
    """Replace one platform's delivery for one date. Returns the rows landed.

    The delete is scoped to `(platform, delivery date)` and the insert is in
    the same transaction, so re-running a morning replaces that morning's
    file for that platform and leaves the other two alone.
    """
    day = _as_date(ds)
    table = warehouse.qualify("raw.ads_spend_daily")
    columns = ", ".join(SPEND_COLUMNS)
    source = delivery_file(platform, day)
    with warehouse.connect() as con:
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(
                f"DELETE FROM {table} WHERE platform = ? AND loaded_at::DATE = ?",
                [platform, day],
            )
            counted = con.execute(
                f"INSERT INTO {table} BY NAME SELECT {columns} "
                f"FROM read_csv('{source}', header = true, union_by_name = true) "
                "WHERE platform = ? AND loaded_at::DATE = ?",
                [platform, day],
            ).fetchall()
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return int(counted[0][0]) if counted and counted[0] else 0


def land_email(ds: str | dt.date) -> int:
    """Replace Larkspur's bulk export for one delivery date.

    Larkspur sends events rather than spend, so its rows go to
    `raw.email_events` and the day is keyed on the event's own time.
    """
    day = _as_date(ds)
    table = warehouse.qualify("raw.email_events")
    source = delivery_file(EMAIL_PLATFORM, day)
    with warehouse.connect() as con:
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(f"DELETE FROM {table} WHERE event_time_utc::DATE = ?", [day])
            counted = con.execute(
                f"INSERT INTO {table} BY NAME SELECT send_id, campaign_id, "
                "customer_ref, email_hash, event_type, event_time_utc, message_id "
                f"FROM read_csv('{source}', header = true, union_by_name = true)",
            ).fetchall()
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return int(counted[0][0]) if counted and counted[0] else 0


def restated_keys(ds: str | dt.date) -> list[dict]:
    """The (platform, report date, campaign) keys this delivery restated.

    A restatement carries a `restated_at`, and the delivery it corrects is
    already in the table. Growth publish this list so that anybody looking at
    a moved number can see when it moved and by how much.
    """
    day = _as_date(ds)
    table = warehouse.qualify("raw.ads_spend_daily")
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT r.platform, r.report_date, r.campaign_id,
                   f.spend_cents AS first_spend_cents,
                   r.spend_cents AS restated_spend_cents
            FROM {table} r
            JOIN {table} f
              ON f.platform = r.platform
             AND f.report_date = r.report_date
             AND f.campaign_id = r.campaign_id
             AND f.restated_at IS NULL
            WHERE r.restated_at IS NOT NULL AND r.loaded_at::DATE = DATE '{day}'
            ORDER BY r.platform, r.report_date, r.campaign_id
            """
        ).fetchall()
    return [
        {"platform": platform, "report_date": str(report_date),
         "campaign_id": campaign_id, "first_spend_cents": int(first or 0),
         "restated_spend_cents": int(restated or 0)}
        for platform, report_date, campaign_id, first, restated in rows
    ]


def report_dates_pending() -> list[str]:
    """Report dates whose newest delivery is newer than the mart's build.

    A restatement three days after the fact makes a closed report date
    pending again, which is the whole reason this list is computed from the
    feed rather than taken as "yesterday".
    """
    spend = warehouse.qualify("raw.ads_spend_daily")
    roi = warehouse.qualify("marts.channel_roi_daily")
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"""
            WITH delivered AS (
                SELECT report_date, max(loaded_at) AS delivered_at
                FROM {spend} GROUP BY report_date
            ), built AS (
                SELECT ds AS report_date, max(built_at) AS built_at
                FROM {roi} GROUP BY ds
            )
            SELECT d.report_date FROM delivered d
            LEFT JOIN built b ON b.report_date = d.report_date
            WHERE b.built_at IS NULL OR b.built_at < d.delivered_at
            ORDER BY d.report_date
            """
        ).fetchall()
    return [row[0].isoformat() for row in rows]


def spend_ties() -> dict:
    """The mart's spend against the feed's, per report date.

    Returns `{"report_dates": n, "gaps": [...]}`. A gap carries the report
    date, the mart's figure and the feed's, so whoever reads it can see the
    size of the difference rather than only that there is one.
    """
    spend = warehouse.qualify("raw.ads_spend_daily")
    roi = warehouse.qualify("marts.channel_roi_daily")
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"""
            WITH latest AS (
                SELECT report_date, platform, campaign_id,
                       arg_max(spend_cents, loaded_at) AS spend_cents
                FROM {spend} GROUP BY report_date, platform, campaign_id
            ), feed AS (
                SELECT report_date, sum(spend_cents) AS spend_cents
                FROM latest GROUP BY report_date
            ), mart AS (
                SELECT ds AS report_date, sum(spend_cents) AS spend_cents
                FROM {roi} GROUP BY ds
            )
            SELECT m.report_date, m.spend_cents, f.spend_cents
            FROM mart m JOIN feed f ON f.report_date = m.report_date
            WHERE m.spend_cents <> f.spend_cents
            ORDER BY m.report_date
            """
        ).fetchall()
        dates = con.execute(f"SELECT count(DISTINCT ds) FROM {roi}").fetchone()
    return {
        "report_dates": int(dates[0] or 0),
        "gaps": [{"report_date": str(report_date),
                  "mart_spend_cents": int(mart or 0),
                  "feed_spend_cents": int(feed or 0)}
                 for report_date, mart, feed in rows],
    }


def missing_rates(ds: str | dt.date) -> list[dict]:
    """Deliveries whose currency has no rate at their report date.

    Spend is billed in the market's own currency and there is no rate on the
    row, so every figure in a shared currency is a join to `raw.fx_rates` at
    the report date. A missing rate is a campaign that quietly drops out of
    that figure, so it is counted here on the way in rather than found later
    in a total that does not tie.
    """
    day = _as_date(ds)
    spend = warehouse.qualify("raw.ads_spend_daily")
    rates = warehouse.qualify("raw.fx_rates")
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT s.currency_code, s.report_date, count(*) AS deliveries
            FROM {spend} s
            LEFT JOIN {rates} r
              ON r.currency_code = s.currency_code
             AND r.rate_date = s.report_date
            WHERE s.loaded_at::DATE = DATE '{day}' AND r.currency_code IS NULL
            GROUP BY 1, 2 ORDER BY 1, 2
            """
        ).fetchall()
    return [{"currency_code": currency, "report_date": str(report_date),
             "deliveries": int(count)}
            for currency, report_date, count in rows]


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
