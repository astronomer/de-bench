-- Priced lines against the order headers they belong to, cent exact.
SELECT count(*)
FROM (
    SELECT p.order_id,
           sum(p.line_total_cents) AS lines_cents,
           max(o.subtotal_cents - o.order_discount_cents + o.tax_cents)
               AS header_cents
    FROM copperline.staging.close_priced p
    JOIN copperline.raw.orders o ON o.order_id = p.order_id
    WHERE o.local_order_date = ?
    GROUP BY p.order_id
    HAVING sum(p.line_total_cents)
         <> max(o.subtotal_cents - o.order_discount_cents + o.tax_cents)
)
