-- REV-6, and the whole of it. Both dates are taken in the boundary zone before
-- the difference is computed, and day fourteen is inside the window.
UPDATE copperline.staging.refunds_day
SET rev6_side = CASE
        WHEN invoice_date IS NULL THEN NULL
        WHEN date_diff('day',
                       invoice_date,
                       (initiated_at AT TIME ZONE 'UTC' AT TIME ZONE ?)::DATE) <= ?
        THEN 'void' ELSE 'cancel' END
WHERE initiated_at::DATE = ?
