SELECT
    (SELECT count(*) FROM copperline.raw.marketplace_settlements
      WHERE payout_date = ?),
    (SELECT count(*) FROM copperline.raw.marketplace_payouts
      WHERE payout_date = ?)
