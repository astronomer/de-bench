-- The week's settlement lines, from both the marketplace remittance and the
-- processor feed. The week is closed at both ends: `week_start` counts and the
-- day after `week_end` does not.
CREATE OR REPLACE TABLE copperline.staging.settlement_lines AS
SELECT s.settlement_id, s.payout_id, s.marketplace_order_id, s.order_id,
       s.line_type, s.amount_cents, s.payout_date,
       p.seller_id, 'marketplace' AS feed
FROM copperline.raw.marketplace_settlements s
JOIN copperline.raw.marketplace_payouts p ON p.payout_id = s.payout_id
WHERE s.payout_date >= ? AND s.payout_date <= ?
