-- One manifest row per store-day loaded, with the pair POS-2 reconciles on.
-- The hourly intake takes the files that landed on the business date itself, so
-- what it writes arrived on time: `ok`. `pos_manifest_build_store.sql` is the
-- catch-up's copy and writes `late`. Those two words and `missing` are the
-- whole of this column's vocabulary — the till reconciliation, the escalation
-- and the store teams all read it, so nothing may add a fourth.
INSERT OR REPLACE INTO copperline.raw.pos_batch_manifest BY NAME
SELECT 'B-' || strftime(business_date, '%Y%m%d') || '-' || store_id AS batch_id,
       store_id, business_date,
       'store_' || store_id || '.csv'      AS file_name,
       count(*)                            AS row_count,
       'ok'                                AS status
FROM copperline.raw.pos_sales_header
WHERE business_date = ?
GROUP BY store_id, business_date
