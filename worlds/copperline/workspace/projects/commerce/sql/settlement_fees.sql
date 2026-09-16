-- Rate each line on the fee schedule in force for the week, never on today's.
UPDATE copperline.staging.settlement_lines l
SET amount_cents = CASE
        WHEN l.line_type = 'commission'
        THEN round(o.gmv_cents * f.commission_rate_bps / 10000.0)
        ELSE l.amount_cents END
FROM copperline.raw.marketplace_orders o
JOIN copperline.staging.fee_schedule f
  ON f.seller_tier = o.seller_id
 AND f.effective_from <= ?
 AND (f.effective_to IS NULL OR f.effective_to > ?)
WHERE o.marketplace_order_id = l.marketplace_order_id
