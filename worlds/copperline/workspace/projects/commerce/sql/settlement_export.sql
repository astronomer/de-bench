-- The file the summary email carries.
SELECT week_start, seller_id, gmv_cents, commission_cents, fee_cents,
       payout_cents
FROM copperline.marts.settlement_weekly
WHERE week_start = ?
ORDER BY seller_id
