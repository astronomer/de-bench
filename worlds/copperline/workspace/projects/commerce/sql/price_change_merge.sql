-- The change feed arrives out of order and a corrected change is re-sent under
-- the same id, so the merge keys on `change_id` and the later row wins.
INSERT OR REPLACE INTO copperline.raw.pim_product_versions BY NAME
SELECT * FROM copperline.staging.pim_changes
