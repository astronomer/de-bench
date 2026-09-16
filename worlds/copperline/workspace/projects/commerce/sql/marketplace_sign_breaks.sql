-- The principal is money out and signs negative; commission and the fee sign
-- positive. A flipped sign is loud in the payout tie and silent everywhere else.
SELECT count(*)
FROM copperline.raw.marketplace_settlements
WHERE payout_date = ?
  AND ((line_type = 'principal' AND amount_cents > 0)
    OR (line_type IN ('commission', 'fulfilment_fee') AND amount_cents < 0))
