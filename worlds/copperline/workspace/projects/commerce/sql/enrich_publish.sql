-- Replace the day's partition of the enriched spine.
INSERT INTO copperline.staging.orders_enriched BY NAME
SELECT b.*, c.customer_key, c.source_book,
       p.line_count, p.unit_count, p.category_count,
       h.is_trading_day, h.holiday_name,
       t.tender_state, t.tendered_cents
FROM copperline.staging.enrich_base b
LEFT JOIN copperline.staging.enrich_customer c ON c.order_id = b.order_id
LEFT JOIN copperline.staging.enrich_product  p ON p.order_id = b.order_id
LEFT JOIN copperline.staging.enrich_channel  h ON h.order_id = b.order_id
LEFT JOIN copperline.staging.enrich_tender   t ON t.order_id = b.order_id
WHERE b.local_order_date = ?
