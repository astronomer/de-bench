-- Pair the day's orders with the day's processor events, through the bridge.
-- docs/billing-integration.md B-6: the text reference on a settlement row is
-- not unique, and `raw.payment_intents.order_id` is.
--
-- A soft-deleted order keeps its last known amounts and stays in the match,
-- per docs/reconciliation-policy.md R-6.
CREATE OR REPLACE TABLE copperline.staging.recon_matched AS
SELECT o.order_id,
       o.local_order_date                    AS ds,
       o.grand_total_cents                   AS order_cents,
       o.order_status,
       s.event_id,
       s.event_type,
       s.amount_cents                        AS processor_cents,
       s.event_time_utc,
       o.deleted_at IS NOT NULL              AS deleted_at_source
FROM copperline.raw.orders o
LEFT JOIN copperline.raw.payment_intents i
       ON i.order_id = try_cast(o.order_id AS BIGINT)
LEFT JOIN copperline.raw.pay_meridian_settlements s
       ON s.intent_id = i.intent_id
      AND s.attempt_no = i.attempt_no
      AND s.deleted_at IS NULL
WHERE o.local_order_date = ?
