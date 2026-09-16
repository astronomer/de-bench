-- Net revenue and merchandise margin at line grain.
CREATE OR REPLACE TABLE copperline.staging.close_line_economics AS
SELECT p.order_id, p.order_line_id,
       p.line_total_cents                                    AS booked_cents,
       p.line_total_cents - coalesce(r.refund_cents, 0)       AS net_sales_cents,
       p.line_total_cents - coalesce(r.refund_cents, 0)
           - round(p.qty * p.unit_cost_cents)                 AS merch_margin_cents
FROM copperline.staging.close_priced p
LEFT JOIN (
    SELECT order_line_id, sum(refund_cents) AS refund_cents
    FROM copperline.raw.returns
    GROUP BY order_line_id
) r ON r.order_line_id = p.order_line_id
WHERE p.order_id IN (
    SELECT order_id FROM copperline.raw.orders WHERE local_order_date = ?
)
