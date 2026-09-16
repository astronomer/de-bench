SELECT count(*)
FROM copperline.raw.pos_batch_manifest
WHERE business_date = ?
  AND coalesce(claimed_row_count, row_count) = row_count
