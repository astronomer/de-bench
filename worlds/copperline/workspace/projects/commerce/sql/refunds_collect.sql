-- The day's refunds, with the invoice date each one refunds against.
CREATE OR REPLACE TABLE copperline.staging.refunds_day AS
SELECT r.rma_id, r.order_id, r.order_line_id, r.refund_cents,
       r.initiated_at, v.invoice_date
FROM copperline.raw.returns r
LEFT JOIN copperline.raw.invoices v ON v.order_id = r.order_id
WHERE r.initiated_at::DATE = ?
