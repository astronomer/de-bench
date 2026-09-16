-- Replace the day's partition of the published exception list.
INSERT INTO copperline.marts.recon_exceptions BY NAME
SELECT order_id, ds, exception_kind, order_cents, processor_cents,
       event_time_utc
FROM copperline.staging.recon_found
WHERE ds = ?
