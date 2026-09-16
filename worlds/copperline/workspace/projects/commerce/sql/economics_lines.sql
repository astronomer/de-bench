-- Discounted and costed lines for the day, from the shared int models. The
-- definitions are finance's too, which is why they are not rebuilt here.
CREATE OR REPLACE TABLE copperline.staging.economics_lines AS
SELECT d.order_id, d.order_line_id, d.order_date,
       d.line_total_cents        AS booked_cents,
       c.landed_cost_cents
FROM copperline.marts.int_order_lines_discounted d
JOIN copperline.marts.int_order_lines_costed c
  ON c.order_line_id = d.order_line_id
WHERE d.order_date = ?
