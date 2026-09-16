-- The general-ledger extract the finance close reads: order grain, integer
-- cents, one row per order.
SELECT order_id, order_date, channel, booked_cents, net_sales_cents
FROM copperline.marts.order_economics
WHERE order_date = ?
ORDER BY order_id
