-- Order ids that appear more than once in a day's cut. Empty, or the cut was
-- taken twice and every total behind it is doubled.
SELECT order_id, count(*) AS copies
FROM copperline.raw.orders
WHERE local_order_date = ?
GROUP BY order_id
HAVING count(*) > 1
ORDER BY copies DESC, order_id
