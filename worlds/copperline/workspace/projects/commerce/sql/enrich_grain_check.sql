-- Orders in, rows out, and how many orders came out more than once.
SELECT
    (SELECT count(*) FROM copperline.raw.orders
      WHERE local_order_date = ? AND NOT is_test),
    (SELECT count(*) FROM copperline.staging.orders_enriched
      WHERE local_order_date = ?),
    (SELECT count(*) FROM (
        SELECT order_id FROM copperline.staging.orders_enriched
         WHERE local_order_date = ?
         GROUP BY order_id HAVING count(*) > 1))
