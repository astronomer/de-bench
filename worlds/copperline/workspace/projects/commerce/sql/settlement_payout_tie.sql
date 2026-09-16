SELECT
    (SELECT coalesce(sum(payout_cents), 0) FROM copperline.marts.settlement_weekly
      WHERE week_start = ?),
    (SELECT coalesce(sum(net_paid_cents), 0) FROM copperline.raw.marketplace_payouts
      WHERE payout_date >= ? AND payout_status = 'paid')
