-- R-4: age every disagreement on the processor's own clock. Under 48 hours is
-- `pending_sync`, which is a state and not an exception, however long ago we
-- loaded it.
UPDATE copperline.staging.recon_found
SET exception_kind = 'pending_sync'
WHERE ds = ?
  AND event_time_utc IS NOT NULL
  AND date_diff('hour', event_time_utc, ds::TIMESTAMP + INTERVAL 1 DAY) < 48
