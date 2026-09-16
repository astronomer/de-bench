-- POS-2: a difference of whole transactions is a partial file. A difference of
-- a few cents in the money is float rounding at the register and is not this.
SELECT store_id, claimed_row_count - row_count AS short
FROM copperline.raw.pos_batch_manifest
WHERE business_date = ?
  AND claimed_row_count IS NOT NULL
  AND row_count IS NOT NULL
  AND claimed_row_count <> row_count
ORDER BY store_id
