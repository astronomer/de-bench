-- Returns netted onto the day that sold them, one row per order day. RTN-266.
--
-- `ds` is the order's own `local_order_date`, so a row belongs to the day the
-- goods were sold and not to the night the authorisation was raised. The write
-- is scoped to `ds`.
CREATE TABLE IF NOT EXISTS copperline.marts.returns_by_order_day (
    ds DATE NOT NULL,
    rmas BIGINT NOT NULL,
    open_rmas BIGINT NOT NULL,
    returned_qty DECIMAL(12,3) NOT NULL,
    refund_cents BIGINT NOT NULL,
    card_refund_cents BIGINT NOT NULL,
    returned_line_cents BIGINT NOT NULL
)
