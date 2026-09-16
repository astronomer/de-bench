-- The published day: one row per market per channel. A day nobody was due to
-- send has no row here, and no partition file beside it either.
CREATE TABLE IF NOT EXISTS copperline.marts.channel_daily (
    ds DATE NOT NULL,
    market_code VARCHAR NOT NULL,
    channel VARCHAR NOT NULL,
    orders BIGINT NOT NULL,
    booked_cents BIGINT NOT NULL
)
