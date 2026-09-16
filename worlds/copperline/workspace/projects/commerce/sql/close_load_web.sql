-- The close date's web rows, into the close's own working table. The
-- delete and the insert are one statement, so the day is never half there.
INSERT OR REPLACE INTO copperline.staging.close_channel
SELECT 'web' AS channel, ? AS ds, count(*) AS rows_in
FROM copperline.raw.orders
WHERE local_order_date = ?
