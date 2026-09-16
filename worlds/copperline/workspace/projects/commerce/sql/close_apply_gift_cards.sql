-- Gift card and store credit are tender, not discount, per REV-12. They come
-- off what was collected and never off what was sold.
CREATE OR REPLACE TABLE copperline.staging.close_tendered AS
SELECT o.order_id,
       o.grand_total_cents,
       o.gift_card_applied_cents,
       o.grand_total_cents - o.gift_card_applied_cents AS collected_cents
FROM copperline.raw.orders o
WHERE o.local_order_date = ?
