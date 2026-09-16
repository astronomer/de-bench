-- Replace this week's rows. Keyed on `week_start`, so re-running a week
-- replaces that week and no other.
INSERT INTO copperline.marts.settlement_weekly BY NAME
SELECT ? AS week_start,
       seller_id,
       sum(CASE WHEN line_type = 'principal' THEN -amount_cents ELSE 0 END)
           AS gmv_cents,
       sum(CASE WHEN line_type = 'commission' THEN amount_cents ELSE 0 END)
           AS commission_cents,
       sum(CASE WHEN line_type = 'fulfilment_fee' THEN amount_cents ELSE 0 END)
           AS fee_cents,
       sum(-amount_cents) AS payout_cents
FROM copperline.staging.settlement_lines
WHERE payout_date >= ?
GROUP BY seller_id
