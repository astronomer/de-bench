-- Resolve the customer reference through the crosswalk, by the format the
-- reference is written in rather than by the order's date. The two formats
-- overlap, which is what docs/runbooks/customer-id-migration.md describes.
CREATE OR REPLACE TABLE copperline.staging.enrich_customer AS
SELECT b.order_id,
       coalesce(m.customer_id, b.loyalty_id) AS customer_key,
       m.source_book
FROM copperline.staging.enrich_base b
LEFT JOIN copperline.raw.customer_id_map m
       ON m.legacy_ref = b.customer_ref
WHERE b.local_order_date = ?
