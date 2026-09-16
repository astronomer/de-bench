-- Priced lines against the order headers they belong to, cent exact.
--
-- The header's `subtotal_cents` IS the lines' total: `line_total_cents` is the
-- line as the OMS priced it, tax sits in the header's `tax_cents`, and the
-- order-level discount sits in `order_discount_cents` and has not been taken off
-- any line. Holding the lines against `subtotal - order_discount + tax` put a
-- tax-exclusive figure against a tax-inclusive one and took the header discount
-- off one side only, so the count could not come out at nought on any order that
-- carried either. Over the whole order book it counts 3,230,670 of 3,436,578
-- orders as breaks, and the step returns that count without raising, so nobody
-- ever read it. (Derivation: `select count(*) from raw.orders o join (select
-- order_id, sum(line_total_cents) lt from raw.order_lines group by 1) l using
-- (order_id) where o.subtotal_cents - o.order_discount_cents + o.tax_cents
-- <> l.lt`.)
SELECT count(*)
FROM (
    SELECT p.order_id
    FROM copperline.staging.close_priced p
    JOIN copperline.raw.orders o ON o.order_id = p.order_id
    WHERE o.local_order_date = ?
    GROUP BY p.order_id
    HAVING sum(p.line_total_cents) <> max(o.subtotal_cents)
)
