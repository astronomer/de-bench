-- Both sides have the payment and the amounts differ.
INSERT INTO copperline.staging.recon_found
SELECT order_id, ds, ? AS exception_kind, order_cents, processor_cents,
       event_time_utc
FROM copperline.staging.recon_matched
WHERE ds = ?
  AND processor_cents IS NOT NULL
  AND order_cents <> processor_cents
