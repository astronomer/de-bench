-- Every order in the day's economics partition appears in exactly one partner
-- file. An order in neither is an order a partner never sees; an order in both
-- is counted twice by whoever adds the two files up.
SELECT count(*)
FROM copperline.marts.order_economics
WHERE order_date = ?
  AND channel NOT IN ('store', 'web', 'trade', 'marketplace')
