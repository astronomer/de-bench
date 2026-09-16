SELECT count(*)
FROM copperline.raw.marketplace_orders o
JOIN copperline.staging.mkt_seller_region r ON r.seller_id = o.seller_id
WHERE o.placed_at::DATE = ? AND r.region = ?
