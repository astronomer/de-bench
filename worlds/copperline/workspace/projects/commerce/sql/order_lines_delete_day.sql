-- The day's lines, by the orders that belong to the day. Lines carry no date.
DELETE FROM copperline.raw.order_lines
WHERE order_id IN (
    SELECT order_id FROM copperline.raw.orders WHERE local_order_date = ?
)
