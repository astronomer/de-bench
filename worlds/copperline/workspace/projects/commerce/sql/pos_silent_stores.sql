-- Stores that have sent nothing for the lookback window, with the last day
-- they did. POS-1 escalates these rather than backfilling them, and it says to
-- check the market calendar first: a store in a market with
-- `feed_expected = false` sent nothing because it was shut.
SELECT s.store_id, max(m.business_date) AS last_seen
FROM copperline.raw.stores s
LEFT JOIN copperline.raw.pos_batch_manifest m
       ON m.store_id = s.store_id AND m.status = 'loaded'
LEFT JOIN copperline.raw.market_calendar c
       ON c.market_code = s.market_code AND c.calendar_date = ?::DATE
WHERE coalesce(c.feed_expected, true)
GROUP BY s.store_id
HAVING max(m.business_date) IS NULL
    OR max(m.business_date) < ?::DATE - CAST(? AS INTEGER)
ORDER BY s.store_id
