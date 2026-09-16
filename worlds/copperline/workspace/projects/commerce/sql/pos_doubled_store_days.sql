SELECT store_id, count(*) AS batches
FROM copperline.raw.pos_batch_manifest
WHERE business_date = ?
GROUP BY store_id
HAVING count(*) > 1
ORDER BY store_id
