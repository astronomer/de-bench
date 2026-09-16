-- The day's Meridian events, each under the order that paid for it.
--
-- The event row carries `order_ref`, and `order_ref` is not a key:
-- docs/billing-integration.md B-6. Seven digits cannot hold the order key, so
-- the reference recycles every fifty days of volume and names about seventeen
-- orders over the range. Joined straight to `raw.orders` on it, one event
-- matches seven.
--
-- The event also carries `intent_id`, and that one is exact — one intent per
-- payment attempt, one attempt per intent. So the event reaches its order
-- through the bridge, and it is the BRIDGE ROW'S clock that picks between the
-- orders sharing the reference: an attempt is raised the day the order is
-- placed, whereas the event can be a refund six weeks later or a chargeback
-- later still. Anchoring the settlement's own `event_time_utc` against the
-- order date attributes every refund and every chargeback to whichever order
-- fifty days away happens to sit nearer.
SELECT CAST(? AS DATE)      AS ds,
       s.event_id,
       s.intent_id,
       o.order_id,
       s.event_type,
       s.amount_cents
FROM copperline.raw.pay_meridian_settlements s
JOIN copperline.raw.payment_intents i
  ON i.intent_id = s.intent_id
JOIN copperline.raw.orders o
  ON o.order_ref = i.order_ref
 AND i.created_at::DATE BETWEEN o.local_order_date - 7 AND o.local_order_date + 7
WHERE s.event_time_utc::DATE = ?
ORDER BY s.event_id
