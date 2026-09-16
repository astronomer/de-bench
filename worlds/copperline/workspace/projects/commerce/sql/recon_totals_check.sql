-- Rows that are neither matched nor on the exception list, and the exception
-- count the squad reads, which excludes `pending_sync`.
SELECT
    (SELECT count(*) FROM copperline.staging.recon_matched m
      WHERE m.ds = ?
        AND m.order_cents IS NULL AND m.processor_cents IS NULL),
    (SELECT count(*) FROM copperline.marts.recon_exceptions
      WHERE ds = ? AND exception_kind <> 'pending_sync')
