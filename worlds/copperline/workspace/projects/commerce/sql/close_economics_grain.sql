SELECT
    (SELECT count(*) FROM copperline.marts.order_economics WHERE order_date = ?),
    (SELECT count(DISTINCT order_id) FROM copperline.marts.order_economics
      WHERE order_date = ?)
