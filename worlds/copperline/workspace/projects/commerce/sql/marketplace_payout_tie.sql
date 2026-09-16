SELECT p.payout_id
FROM copperline.raw.marketplace_payouts p
JOIN (
    SELECT payout_id, sum(-amount_cents) AS lines_cents
    FROM copperline.raw.marketplace_settlements
    WHERE payout_date = ?
    GROUP BY payout_id
) s ON s.payout_id = p.payout_id
WHERE p.net_paid_cents <> s.lines_cents
ORDER BY p.payout_id
