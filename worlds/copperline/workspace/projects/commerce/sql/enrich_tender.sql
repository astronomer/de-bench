-- One tender state per order, taken from the processor rather than from the
-- order's own status. docs/reconciliation-policy.md R-1: for anything about
-- money the processor is the system of record.
CREATE OR REPLACE TABLE copperline.staging.enrich_tender AS
SELECT b.order_id,
       max(s.event_type)     AS tender_state,
       sum(s.amount_cents)   AS tendered_cents,
       count(*)              AS event_count
FROM copperline.staging.enrich_base b
LEFT JOIN copperline.raw.payment_intents i ON i.order_id = try_cast(b.order_id AS BIGINT)
LEFT JOIN copperline.raw.pay_meridian_settlements s
       ON s.intent_id = i.intent_id
      AND s.attempt_no = i.attempt_no
      AND s.deleted_at IS NULL
WHERE b.local_order_date = ?
GROUP BY b.order_id
