SELECT
    (SELECT count(*) FROM copperline.marts.settlement_weekly WHERE week_start = ?),
    (SELECT count(DISTINCT seller_id) FROM copperline.marts.settlement_weekly
      WHERE week_start = ?)
