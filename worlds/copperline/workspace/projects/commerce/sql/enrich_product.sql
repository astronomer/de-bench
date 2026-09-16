-- The product attributes in force on the order date, from the snapshot pair.
CREATE OR REPLACE TABLE copperline.staging.enrich_product AS
SELECT l.order_id,
       count(*)                       AS line_count,
       sum(l.qty)                     AS unit_count,
       count(DISTINCT p.category_id)  AS category_count
FROM copperline.raw.order_lines l
JOIN copperline.staging.enrich_base b ON b.order_id = l.order_id
LEFT JOIN copperline.marts.dim_product p
       ON p.sku = l.sku
      AND b.local_order_date >= p.valid_from
      AND (p.valid_to IS NULL OR b.local_order_date < p.valid_to)
WHERE b.local_order_date = ?
GROUP BY l.order_id
