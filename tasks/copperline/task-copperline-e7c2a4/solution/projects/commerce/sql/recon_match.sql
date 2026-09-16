-- Pair the day's orders with the day's processor events, through the bridge.
--
-- THE ORDER SIDE OF THE BRIDGE IS `order_ref` PLUS THE ATTEMPT'S OWN DATE.
-- `raw.payment_intents.order_id` is the processor's own counter and not ours.
-- Our keys are `ORD-########` and the numbers inside them start at 1; that
-- column's numbers start far above where ours stop, so the two sets never meet,
-- however the text is cast. `stg_payments__payment_intents` already spells the
-- column `processor_order_key`, which is what it is. This statement read it as
-- our key and matched nothing at all, every night, for as long as it has run.
--
-- `order_ref` on its own is not a key either: the OMS recycles a seven-digit
-- reference about every fifty days, so one reference names several orders over
-- the range (docs/billing-integration.md B-6). An attempt is raised a day or
-- two after its order, so the reference plus a week either side names exactly
-- one of them. `int_orders_enriched` matches orders to attempts by that rule
-- and this is the same rule in the same shape.
--
-- `intent_id` carries the attempt the rest of the way to the settlement, which
-- is the part B-6 was always right about.
--
-- Both joins stay LEFT. An order the processor never saw is R-7's `no_payment`
-- and has to reach the finder rather than fall out here.
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
       ON i.order_ref = o.order_ref
      AND i.created_at::DATE BETWEEN o.local_order_date - 7
                                 AND o.local_order_date + 7
LEFT JOIN copperline.raw.pay_meridian_settlements s
       ON s.intent_id = i.intent_id
      AND s.attempt_no = i.attempt_no
      AND s.deleted_at IS NULL
WHERE o.local_order_date = ?
