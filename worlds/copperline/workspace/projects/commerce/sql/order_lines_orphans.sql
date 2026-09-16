-- Lines whose order is not in the day's headers.
SELECT count(*)
FROM copperline.raw.order_lines l
LEFT JOIN copperline.raw.orders o ON o.order_id = l.order_id
WHERE o.order_id IS NULL
  AND l.order_id IN (
      SELECT order_id FROM copperline.raw.orders WHERE local_order_date = ?
  )
