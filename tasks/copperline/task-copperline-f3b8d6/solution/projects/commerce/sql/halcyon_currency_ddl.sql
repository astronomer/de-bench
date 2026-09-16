-- One row per Halcyon settlement, in the currency it was taken in.
--
-- `raw.pay_halcyon_settlements` states the currency on about two thirds of its
-- rows and leaves the column empty on the rest, and it lands the amount as a
-- two-decimal string. This table is the feed with both of those settled: a
-- currency on every row, and the amount as integer cents.
CREATE TABLE IF NOT EXISTS copperline.ops.halcyon_settlement_currency (
    txn_id VARCHAR NOT NULL,
    currency_code VARCHAR NOT NULL,
    amount_cents BIGINT NOT NULL,
    settled_on DATE NOT NULL
)
