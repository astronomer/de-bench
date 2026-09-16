-- The close date's rows for the published mart, in the contract's order.
--
-- This was a bare `INSERT INTO copperline.marts.order_economics`, which
-- `CONVENTIONS.md` calls a defect in as many words: the second run of a date
-- puts the day in twice and the grain the merch dashboards reconcile on is gone.
-- The statement now only selects; `publish_economics` writes it through
-- `warehouse.delete_insert`, which owns the date and no other.
SELECT order_id, order_date, channel, booked_cents, net_sales_cents,
       merch_margin_cents
FROM copperline.staging.close_order_economics
WHERE order_date = ?
