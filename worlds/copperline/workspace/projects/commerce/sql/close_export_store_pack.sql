-- The store pack: one row per store, sent to each store in the morning.
SELECT store_id,
       count(*)                        AS orders,
       sum(grand_total_cents)          AS booked_cents
FROM copperline.staging.orders_enriched
WHERE local_order_date = ? AND store_id IS NOT NULL
GROUP BY store_id
ORDER BY store_id
