-- R-7, both directions. An order with no processor event is `no_payment`; a
-- processor event with no order is `no_order`. Neither is netted against the
-- other and neither is written off.
INSERT INTO copperline.staging.recon_found
SELECT order_id, ds,
       CASE WHEN event_id IS NULL THEN 'no_payment' ELSE ? END AS exception_kind,
       order_cents, processor_cents, event_time_utc
FROM copperline.staging.recon_matched
WHERE ds = ?
  AND (event_id IS NULL OR order_id IS NULL)
