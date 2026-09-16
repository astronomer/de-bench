SELECT DISTINCT line_type
FROM copperline.raw.marketplace_settlements
WHERE payout_date = ?
  AND line_type NOT IN ('principal', 'commission', 'fulfilment_fee', 'refund')
ORDER BY line_type
