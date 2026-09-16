INSERT INTO copperline.marts.refunds_daily BY NAME
SELECT ? AS ds, rma_id, order_id, order_line_id, refund_cents, invoice_date,
       rev6_side
FROM copperline.staging.refunds_day
WHERE initiated_at::DATE = ?
