SELECT
    (SELECT count(*) FROM copperline.raw.pay_meridian_settlements
      WHERE settlement_date = ?),
    (SELECT count(*) FROM copperline.raw.pay_halcyon_settlements
      WHERE file_date = ?)
