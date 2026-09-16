-- R-3: while a dispute is open, its state overrides both sides and the amount
-- is unknown until the case closes.
UPDATE copperline.staging.recon_matched m
SET event_type = 'disputed', processor_cents = NULL
FROM copperline.raw.disputes d
JOIN copperline.raw.invoices v ON v.invoice_id = d.invoice_id
WHERE v.order_id = m.order_id
  AND d.resolved_on IS NULL
  AND m.ds = ?
