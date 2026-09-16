-- The day as the table holds it, against the day as the feed holds it.
-- Rows written, distinct events written, rows with no order, and the feed's
-- own count for the same event date.
SELECT (SELECT count(*) FROM copperline.marts.settlement_attribution WHERE ds = ?),
       (SELECT count(DISTINCT event_id) FROM copperline.marts.settlement_attribution WHERE ds = ?),
       (SELECT count(*) FROM copperline.marts.settlement_attribution
         WHERE ds = ? AND order_id IS NULL),
       (SELECT count(*) FROM copperline.raw.pay_meridian_settlements
         WHERE event_time_utc::DATE = ?)
