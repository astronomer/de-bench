-- R-5: a correction whose day sits in a closed month books one adjustment row
-- on the 1st of the earliest open month instead of restating the closed one.
INSERT INTO copperline.staging.recon_found
SELECT f.order_id,
       date_trunc('month', c.first_open_day)      AS ds,
       'closed_period_adjustment'                 AS exception_kind,
       f.order_cents, f.processor_cents, f.event_time_utc
FROM copperline.staging.recon_found f
JOIN copperline.staging.closed_periods c ON f.ds <= c.last_closed_day
WHERE f.ds = ?
