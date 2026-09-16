-- Returns split by the channel that sold them, one row per order day and
-- channel. RTN-284.
--
-- `ds` is the order's own `local_order_date`, so a row belongs to the day the
-- goods were sold. `channel` is the ORDER's channel, which is the channel that
-- took the sale; `raw.returns.return_channel` is the door the goods came back
-- through and is a different question, answered by `counter_credit_cents`.
--
-- The write is scoped to `ds`, so a run owns one order day and no other.
CREATE TABLE IF NOT EXISTS copperline.marts.returns_by_channel_day (
    ds DATE NOT NULL,
    channel VARCHAR NOT NULL,
    rmas BIGINT NOT NULL,
    returned_qty DECIMAL(12,3) NOT NULL,
    refund_cents BIGINT NOT NULL,
    card_refund_cents BIGINT NOT NULL,
    store_credit_cents BIGINT NOT NULL,
    counter_credit_cents BIGINT NOT NULL,
    returned_line_cents BIGINT NOT NULL
)
