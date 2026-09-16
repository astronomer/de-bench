-- Returns whose order line does not exist. A return with nothing to return
-- against nets off nothing and quietly overstates net revenue.
SELECT count(*)
FROM copperline.staging.returns_landing r
LEFT JOIN copperline.raw.order_lines l ON l.order_line_id = r.order_line_id
WHERE l.order_line_id IS NULL AND r.initiated_at::DATE = ?
