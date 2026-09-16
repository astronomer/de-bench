-- The catch-up's copy of the manifest build, for one store-day. It runs at
-- 04:00 over business dates that have already passed, so a file it picks up
-- arrived after its date: `late`, which is the word POS-1 uses for exactly
-- this store-day and the word the landed table already holds.
INSERT OR REPLACE INTO copperline.raw.pos_batch_manifest BY NAME
SELECT 'B-' || strftime(business_date, '%Y%m%d') || '-' || store_id AS batch_id,
       store_id, business_date,
       'store_' || store_id || '.csv'      AS file_name,
       count(*)                            AS row_count,
       'late'                              AS status
FROM copperline.raw.pos_sales_header
WHERE business_date = ? AND store_id = ?
GROUP BY store_id, business_date
