-- What the source says it sent for the close date, and what landed.
SELECT max(claimed_rows), max(rows_in)
FROM copperline.staging.close_channel
WHERE ds = ? AND channel = ?
