-- Lines whose stated total does not equal their own arithmetic, to the cent.
SELECT l.order_line_id
FROM copperline.raw.order_lines l
JOIN copperline.raw.orders o ON o.order_id = l.order_id
WHERE o.local_order_date = ?
  AND l.line_total_cents <> round(l.qty * l.unit_price_cents)
                            - l.line_discount_cents + l.tax_cents
ORDER BY l.order_line_id
