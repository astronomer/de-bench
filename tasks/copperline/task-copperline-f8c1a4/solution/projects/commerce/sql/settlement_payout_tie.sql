-- The week's summary against the week's payouts. Both sides are closed at both
-- ends: the summary by `week_start`, the payouts by the first and last day of
-- that same week.
--
-- The payout side used to open at the week and close nowhere, so it summed
-- every payout from the week onwards and the two sides compared different
-- populations.
SELECT
    (SELECT coalesce(sum(payout_cents), 0) FROM copperline.marts.settlement_weekly
      WHERE week_start = ?),
    (SELECT coalesce(sum(net_paid_cents), 0) FROM copperline.raw.marketplace_payouts
      WHERE payout_date >= ? AND payout_date <= ? AND payout_status = 'paid')
