-- Attach the list price each line should have carried, so the realisation
-- figures have a denominator.
--
-- The close prices the population the enrichment built: a staff transaction is
-- out, and an order the source soft-deleted stays, per
-- `docs/reconciliation-policy.md` R-6. That is what `enrich_base` does two steps
-- earlier. Pricing every row of `raw.orders` put staff transactions into the
-- mart and left `close_revenue_tie` holding one population against another.
CREATE OR REPLACE TABLE copperline.staging.close_priced AS
SELECT l.order_id, l.order_line_id, l.sku, l.qty,
       l.unit_price_cents, l.unit_cost_cents, l.line_discount_cents,
       l.tax_cents, l.line_total_cents,
       p.list_price_cents
FROM copperline.raw.order_lines l
JOIN copperline.raw.orders o ON o.order_id = l.order_id
LEFT JOIN copperline.staging.close_price_book p ON p.sku = l.sku
WHERE o.local_order_date = ?
  AND NOT o.is_test
