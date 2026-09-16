-- Returns whose order line does not exist. A return with nothing to return
-- against nets off nothing and quietly overstates net revenue.
--
-- The feed cites the order and the line ordinal, so the line is found on the
-- pair. `order_line_id` is an ordinal and repeats in every order, so matching
-- on it alone finds a line for every authorisation and the count can never come
-- out at anything but nought.
SELECT count(*)
FROM copperline.staging.returns_landing r
LEFT JOIN copperline.raw.order_lines l
       ON l.order_id = r.order_id AND l.order_line_id = r.order_line_id
WHERE l.order_line_id IS NULL AND r.initiated_at::DATE = ?
