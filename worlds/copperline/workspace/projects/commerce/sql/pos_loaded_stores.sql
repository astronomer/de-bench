SELECT DISTINCT store_id
FROM copperline.raw.pos_batch_manifest
WHERE business_date = ? AND status = 'loaded'
