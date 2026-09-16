-- Net matched returns off the order they came from, on the order's own date.
-- A return that arrives later does not move the day it belongs to.
UPDATE copperline.staging.economics_order e
SET net_sales_cents = e.booked_cents - coalesce(n.refund_cents, 0)
FROM (
    SELECT r.order_id, sum(r.refund_cents) AS refund_cents
    FROM copperline.marts.int_net_sales_lines r
    GROUP BY r.order_id
) n
WHERE n.order_id = e.order_id AND e.order_date = ?
