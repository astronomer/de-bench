INSERT INTO copperline.marts.fct_returns BY NAME
SELECT ? AS ds, r.rma_id, r.order_id, r.order_line_id, r.sku, r.qty,
       r.return_reason, r.disposition, r.refund_cents, r.restock_flag
FROM copperline.raw.returns r
WHERE r.initiated_at::DATE = ?
