-- Replace the close date's partition of the published mart.
INSERT INTO copperline.marts.order_economics BY NAME
SELECT order_id, order_date, channel, booked_cents, net_sales_cents,
       merch_margin_cents
FROM copperline.staging.close_order_economics
WHERE order_date = ?
