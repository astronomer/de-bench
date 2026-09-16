SELECT count(*)
FROM copperline.raw.order_lines l
JOIN copperline.raw.orders o ON o.order_id = l.order_id
WHERE o.local_order_date = ?
