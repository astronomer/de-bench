-- The day the close has just built, against the order book it came from.
--
-- Three things were wrong here and the step could not have passed with any two
-- of them fixed.
--
-- It read `marts.order_economics`, which `publish_economics` writes two steps
-- below it. On a date that had never been closed the booked side was nought and
-- the tie raised on the whole day. The figure the close has to stand behind is
-- the one it is about to publish, so the tie holds the build.
--
-- It held the booked total against `grand_total_cents`, which carries the tax,
-- the shipping and the order-level discount. `booked_cents` is the order on the
-- order date, gross — `models/shared/intermediate/int_orders_enriched.sql` takes
-- it off `subtotal_cents` and says so in as many words — and `subtotal_cents` is
-- the total of the line totals on every order in the book.
--
-- And the two sides counted different orders: the mart carried every order of
-- the day, staff transactions included, while this side dropped them. Both sides
-- now read the population `enrich_base` builds — staff out, soft-deleted in, per
-- `docs/reconciliation-policy.md` R-6.
SELECT
    (SELECT coalesce(sum(booked_cents), 0)
       FROM copperline.staging.close_order_economics
      WHERE order_date = ?),
    (SELECT coalesce(sum(subtotal_cents), 0) FROM copperline.raw.orders
      WHERE local_order_date = ? AND NOT is_test)
