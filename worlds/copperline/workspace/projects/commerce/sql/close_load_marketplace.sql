-- The close date's marketplace rows, into the close's own working table. The
-- delete and the insert are one statement, so the day is never half there.
INSERT OR REPLACE INTO copperline.staging.close_channel
SELECT 'marketplace' AS channel, ? AS ds, count(*) AS rows_in
FROM copperline.raw.marketplace_orders
WHERE ship_confirmed_at::DATE = ?
