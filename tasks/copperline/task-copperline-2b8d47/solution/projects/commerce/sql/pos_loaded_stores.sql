-- The stores whose batch for a business date is already in. `ok` and `late`
-- both mean the file arrived and was loaded; `missing` is the row the manifest
-- keeps for a batch that never came, and that store-day is still owed.
SELECT DISTINCT store_id
FROM copperline.raw.pos_batch_manifest
WHERE business_date = ? AND status IN ('ok', 'late')
