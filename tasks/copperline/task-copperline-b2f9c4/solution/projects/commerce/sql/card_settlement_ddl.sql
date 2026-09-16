-- The daily card settlement figure, one row per settlement day per currency.
-- PAY-251.
--
-- `ds` is the event's own date, so the day a row belongs to is the day the
-- payment happened and not the night the feed delivered it. The write is
-- scoped to `ds`.
CREATE TABLE IF NOT EXISTS copperline.marts.card_settlement_daily (
    ds DATE NOT NULL,
    currency_code VARCHAR NOT NULL,
    events BIGINT NOT NULL,
    captured_cents BIGINT NOT NULL,
    refunded_cents BIGINT NOT NULL,
    chargeback_cents BIGINT NOT NULL
)
