SELECT
    (SELECT coalesce(sum(booked_cents), 0) FROM copperline.marts.order_economics
      WHERE order_date = ?),
    (SELECT coalesce(sum(grand_total_cents), 0) FROM copperline.raw.orders
      WHERE local_order_date = ? AND NOT is_test)
