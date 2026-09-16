-- One bridge row per (order, attempt). A settlement joined to an order without
-- the attempt number lands on an arbitrary attempt, which is the thing this
-- model exists to stop.
SELECT count(*)
FROM (
    SELECT order_id, attempt_no
    FROM copperline.marts.stg_payment_intents
    WHERE created_at::DATE = ?
    GROUP BY order_id, attempt_no
    HAVING count(*) > 1
)
