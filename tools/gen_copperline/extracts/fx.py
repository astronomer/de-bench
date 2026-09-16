"""`raw.fx_rates` — the daily rate table every money conversion reads.

Spec 03 section 15 fixes the grain and the unit: one row per date per
currency, and the rate as `rate_to_usd_ppm`, an integer count of parts per
million. Seven currencies over the 862 days of the fixture range is 6,034
rows, which is the number the chapter states.

WHY PARTS PER MILLION. `docs/finance-policy.md` pins the arithmetic — convert
per row, round half up to the cent, then sum — and an integer rate is what
makes that reproducible. Two decimal places cannot hold MXN, BRL or IDR at
all: one Indonesian rupiah is 63 millionths of a dollar, so the rate rounds
to 0.00 in a DECIMAL(18,2) and to 63 in parts per million. The cost is that
IDR carries about 1.6% of resolution per unit, which is the price of one unit
for every currency and is what the chapter chose.

USD HAS NO ROW. The table quotes foreign currency into USD, so a USD row
would be a constant 1,000,000 and a model that joins for it would silently
drop every USD fact when the join misses. There is no USD row anywhere in
`sim_core.exchange_rates` either, so the absence is the same on both sides.

`published_at` is the morning after the rate date, 05:00 UTC, which is when
`fin_fx_rates_intake` runs (spec 05). A model that applies today's rate to
today's orders is reading a rate that did not exist yet — the trap FIN-311
grades is the one next door (settlement date against recognition date), but
the publication clock is here and is honest.
"""

from __future__ import annotations

from ..config import Context

# The hour the rate file lands, the morning after the rate date.
PUBLISHED_HOUR = 5


def build(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.fx_rates (
            rate_date DATE NOT NULL,
            currency_code VARCHAR NOT NULL,
            rate_to_usd_ppm BIGINT NOT NULL,
            source VARCHAR NOT NULL,
            published_at TIMESTAMP NOT NULL,
            PRIMARY KEY (rate_date, currency_code)
        )
    """)
    ctx.sql(f"""
        INSERT INTO raw.fx_rates
        SELECT
            x.rate_date,
            x.from_currency                                       AS currency_code,
            round(x.rate * 1000000)::BIGINT                       AS rate_to_usd_ppm,
            'ironwood_daily'                                      AS source,
            (x.rate_date + INTERVAL 1 DAY)::TIMESTAMP
                + INTERVAL {PUBLISHED_HOUR} HOUR                  AS published_at
        FROM sim_core.exchange_rates x
        WHERE x.to_currency = 'USD'
          AND x.rate_date BETWEEN DATE '{ctx.start}' AND DATE '{ctx.end}'
        ORDER BY x.rate_date, x.from_currency
    """)
