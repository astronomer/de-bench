-- Meridian's event id is its idempotency key. A repeat means an hour's file
-- was taken twice.
SELECT event_id, count(*) AS copies
FROM copperline.raw.pay_meridian_settlements
WHERE settlement_date = ?
GROUP BY event_id
HAVING count(*) > 1
ORDER BY copies DESC, event_id
