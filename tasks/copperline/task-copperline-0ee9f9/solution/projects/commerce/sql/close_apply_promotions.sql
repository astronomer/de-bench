-- Push the header discount down to the lines that were eligible for it, in the
-- order the promotions were applied. docs/finance-policy.md REV-13 governs
-- stacking; this applies the order it states and does not choose one.
--
-- The register cites the order and the line ordinal, so an application lands on
-- the pair. `order_line_id` is an ordinal and repeats in every order, so
-- matching on it alone puts the day's whole L-01 discount on every L-01 line.
-- What this adds is MER-312's question, not this statement's: a header
-- application carries no ordinal and still lands nowhere.
UPDATE copperline.staging.close_priced p
SET line_discount_cents = p.line_discount_cents + coalesce(a.discount_cents, 0)
FROM (
    SELECT order_id, order_line_id, sum(discount_cents) AS discount_cents
    FROM copperline.raw.promo_applications
    WHERE order_id IN (
        SELECT order_id FROM copperline.raw.orders WHERE local_order_date = ?
    )
    GROUP BY order_id, order_line_id
) a
WHERE a.order_id = p.order_id AND a.order_line_id = p.order_line_id
