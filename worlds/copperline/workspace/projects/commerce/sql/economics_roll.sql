CREATE OR REPLACE TABLE copperline.staging.economics_order AS
SELECT l.order_id, l.order_date,
       max(o.channel)                                  AS channel,
       sum(l.booked_cents)                             AS booked_cents,
       sum(l.booked_cents - l.landed_cost_cents)       AS merch_margin_cents
FROM copperline.staging.economics_lines l
JOIN copperline.raw.orders o ON o.order_id = l.order_id
WHERE l.order_date = ?
GROUP BY l.order_id, l.order_date
