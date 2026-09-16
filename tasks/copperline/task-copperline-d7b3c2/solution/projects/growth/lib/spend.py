"""Channel spend, and the platforms' claimed return, in base currency.

`raw.ads_spend_daily` is one row per DELIVERY, in the currency the platform
billed. Two things have to happen before the review can add a day up.

**Resolve the delivery.** The same report date arrives again three to seven
days later with different numbers, so the day is grouped to its key —
platform, campaign, market, channel — and the newest delivery of that key
wins. `projects/growth/lib/ads.py` describes the feed; this module only reads
it.

**Convert once, at the report date.** The rate for a day is the rate for the
day the spend happened on, never the morning the file arrived. A restatement
delivered on the 20th for the 13th still converts at the 13th's rate, so a
report date rebuilt a week later comes out at the same figure it did the first
time.

`raw.fx_rates` holds `rate_to_usd_ppm`, integer parts per million against USD,
one row per currency per day. **USD is the base and has no row there.** That is
not a gap: a dollar is a dollar, and the implicit rate is 1,000,000 ppm. Half
the feed bills in dollars, so an inner join to the rate table silently deletes
half the spend and the total still looks like money. `dim_currency` and
`stg_reference__fx_rates` mint the same USD row for the same reason.

A currency that is neither USD nor in the rate table IS a gap, and this raises
rather than convert it at par.

The arithmetic multiplies first and divides once, on integers, so the figure is
reproducible from the row: `spend_cents`, `fx_rate_ppm` and `fx_rate_date` all
ride along beside the result, which is the shape every converting fact in the
warehouse carries.
"""

from __future__ import annotations

import datetime as dt

from include.lib import warehouse

__all__ = ["TABLE", "BASE_CURRENCY", "BASE_RATE_PPM", "COLUMNS", "resolved_day",
           "build_day"]

#: Where the review reads the day from.
TABLE = "marts.channel_spend_base_daily"

#: The reporting currency, and the rate a row in it converts at. `raw.fx_rates`
#: holds no USD row because there is nothing to publish: 1 USD is 1 USD.
BASE_CURRENCY = "USD"
BASE_RATE_PPM = 1_000_000

#: The published grain and shape, in order.
COLUMNS = (
    "ds", "platform", "campaign_id", "market_code", "channel", "currency_code",
    "fx_rate_ppm", "fx_rate_date", "spend_cents", "spend_base_cents",
    "attributed_revenue_cents", "attributed_revenue_base_cents",
)

#: The mart is new and nothing else in the tree creates it, so the build stands
#: it up on the way past. `IF NOT EXISTS`, never `OR REPLACE`: the table holds
#: every report date and this call owns one of them.
SCHEMA_DDL = "CREATE SCHEMA IF NOT EXISTS {schema}"

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
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

#: One report date, resolved to one delivery per key and converted at that
#: date's rate. `//` is integer division, so nothing here ever holds a float:
#: multiply by the rate, add half a unit, divide once.
DAY_SQL = """
WITH latest AS (
    SELECT report_date, platform, campaign_id, market_code, channel,
           arg_max(currency_code, loaded_at)            AS currency_code,
           arg_max(spend_cents, loaded_at)              AS spend_cents,
           arg_max(attributed_revenue_cents, loaded_at) AS attributed_revenue_cents
    FROM {spend}
    WHERE report_date = DATE '{day}'
    GROUP BY 1, 2, 3, 4, 5
), rated AS (
    SELECT l.*,
           CASE WHEN l.currency_code = '{base}' THEN {base_ppm}
                ELSE r.rate_to_usd_ppm END              AS fx_rate_ppm
    FROM latest l
    LEFT JOIN {rates} r
      ON r.currency_code = l.currency_code
     AND r.rate_date = l.report_date
)
SELECT report_date                                      AS ds,
       platform,
       campaign_id,
       market_code,
       channel,
       currency_code,
       fx_rate_ppm,
       report_date                                      AS fx_rate_date,
       spend_cents,
       CAST((CAST(spend_cents AS HUGEINT) * fx_rate_ppm + 500000) // 1000000
            AS BIGINT)                                  AS spend_base_cents,
       attributed_revenue_cents,
       CAST((CAST(attributed_revenue_cents AS HUGEINT) * fx_rate_ppm + 500000) // 1000000
            AS BIGINT)                                  AS attributed_revenue_base_cents
FROM rated
ORDER BY platform, campaign_id
"""


def resolved_day(ds: str | dt.date, con=None) -> list[dict]:
    """One report date, one row per campaign, converted. Reads nothing else.

    Kept separate from the write so that the review can look at a day without
    touching the mart. Pass `con` to read on a connection already open;
    without one this takes the read-only snapshot.
    """
    day = _as_date(ds)
    sql = DAY_SQL.format(
        spend=warehouse.qualify("raw.ads_spend_daily"),
        rates=warehouse.qualify("raw.fx_rates"),
        base=BASE_CURRENCY,
        base_ppm=BASE_RATE_PPM,
        day=day,
    )
    if con is not None:
        rows = [dict(zip(COLUMNS, row)) for row in con.execute(sql).fetchall()]
    else:
        with warehouse.connect(read_only=True) as reader:
            rows = [dict(zip(COLUMNS, row))
                    for row in reader.execute(sql).fetchall()]
    _refuse_unrated(rows, day)
    return rows


def build_day(ds: str | dt.date) -> int:
    """Publish one report date. Returns the rows written.

    The write is `delete_insert` on `ds`, so a report date rebuilt after a
    restatement replaces itself and leaves every other date alone. The read
    and the write share one connection: the day is about to be written, so
    there is nothing for the read-only snapshot to protect.
    """
    day = _as_date(ds)
    with warehouse.connect() as con:
        con.execute(SCHEMA_DDL.format(schema=warehouse.qualify("marts")))
        con.execute(TABLE_DDL.format(table=warehouse.qualify(TABLE)))
        return warehouse.delete_insert(TABLE, "ds", day,
                                       resolved_day(day, con=con),
                                       columns=list(COLUMNS), con=con)


def _refuse_unrated(rows: list[dict], day: dt.date) -> None:
    """A currency that is neither the base nor in the rate table stops the day.

    Converting it at par would put the right number of the wrong currency into
    a dollar column, and the day would still add up.
    """
    unrated = sorted({row["currency_code"] for row in rows
                      if row["fx_rate_ppm"] is None})
    if unrated:
        raise ValueError(
            f"{day}: no rate for {', '.join(unrated)} in raw.fx_rates, and "
            f"{BASE_CURRENCY} is the only currency that converts without one"
        )


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
