INSERT OR REPLACE INTO copperline.raw.pos_batch_manifest BY NAME
SELECT 'B-' || strftime(business_date, '%Y%m%d') || '-' || store_id AS batch_id,
       store_id, business_date,
       'store_' || store_id || '.csv'      AS file_name,
       count(*)                            AS row_count,
       'loaded'                            AS status
FROM copperline.raw.pos_sales_header
WHERE business_date = ? AND store_id = ?
GROUP BY store_id, business_date
