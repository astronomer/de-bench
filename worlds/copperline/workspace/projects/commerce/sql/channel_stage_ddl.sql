-- One row per market per channel per day, as each market's file lands. The key
-- is what makes a reload of a market replace that market rather than add to it.
CREATE TABLE IF NOT EXISTS copperline.staging.channel_market (
    ds DATE NOT NULL,
    market_code VARCHAR NOT NULL,
    channel VARCHAR NOT NULL,
    orders BIGINT NOT NULL,
    booked_cents BIGINT NOT NULL,
    PRIMARY KEY (ds, market_code, channel)
)
