-- Collapse the lines to one row per order.
CREATE OR REPLACE TABLE copperline.staging.close_order_economics AS
SELECT e.order_id,
       o.local_order_date            AS order_date,
       o.channel,
       sum(e.booked_cents)           AS booked_cents,
       sum(e.net_sales_cents)        AS net_sales_cents,
       sum(e.merch_margin_cents)     AS merch_margin_cents
FROM copperline.staging.close_line_economics e
JOIN copperline.raw.orders o ON o.order_id = e.order_id
WHERE o.local_order_date = ?
GROUP BY e.order_id, o.local_order_date, o.channel
