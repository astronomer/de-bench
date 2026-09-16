-- Orders the settlement feed names that the order load does not have. The
-- listing reports no total, so this is the only count there is to check
-- against.
SELECT count(DISTINCT s.marketplace_order_id)
FROM copperline.raw.marketplace_settlements s
LEFT JOIN copperline.raw.marketplace_orders o
       ON o.marketplace_order_id = s.marketplace_order_id
WHERE s.payout_date >= ?::DATE - INTERVAL 7 DAY
  AND s.payout_date <= ?::DATE
  AND o.marketplace_order_id IS NULL
