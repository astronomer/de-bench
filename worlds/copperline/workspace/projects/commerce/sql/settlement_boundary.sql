-- docs/billing-integration.md B-8: over the quarter when both processors were
-- live the same money reaches us twice. Keep the authoritative side only.
DELETE FROM copperline.staging.settlement_lines
WHERE payout_date >= ? AND payout_date <= ?
  AND feed = 'processor'
  AND order_id IN (
      SELECT order_id FROM copperline.staging.settlement_lines
       WHERE feed = 'marketplace'
  )
