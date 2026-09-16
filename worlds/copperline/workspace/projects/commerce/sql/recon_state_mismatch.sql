-- Both sides have the payment and the states differ. R-1 takes the processor's.
INSERT INTO copperline.staging.recon_found
SELECT order_id, ds, ? AS exception_kind, order_cents, processor_cents,
       event_time_utc
FROM copperline.staging.recon_matched
WHERE ds = ?
  AND event_type IS NOT NULL
  AND event_type <> order_status
  AND order_cents = coalesce(processor_cents, order_cents)
