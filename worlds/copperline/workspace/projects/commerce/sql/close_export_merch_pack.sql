-- The merchandising pack: margin by channel for the close date.
SELECT channel,
       count(*)                        AS orders,
       sum(booked_cents)               AS booked_cents,
       sum(net_sales_cents)            AS net_sales_cents,
       sum(merch_margin_cents)         AS merch_margin_cents
FROM copperline.marts.order_economics
WHERE order_date = ?
GROUP BY channel
ORDER BY channel
