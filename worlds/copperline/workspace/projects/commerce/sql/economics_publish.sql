-- Replace the day's partition.
INSERT INTO copperline.marts.order_economics BY NAME
SELECT order_id, order_date, channel, booked_cents,
       coalesce(net_sales_cents, booked_cents) AS net_sales_cents,
       merch_margin_cents
FROM copperline.staging.economics_order
WHERE order_date = ?
