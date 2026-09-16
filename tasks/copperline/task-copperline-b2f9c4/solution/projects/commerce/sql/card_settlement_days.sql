-- The settlement days one night's delivery touched, oldest first.
--
-- Meridian delivers hourly and about 92% of a day's events arrive on the day
-- they happened; the rest arrive over the following five days
-- (`docs/billing-integration.md` B-5). So a night carries rows for six
-- settlement days, and those are the days this run owes a rebuild.
--
-- Taken from the delivery rather than from a fixed number of days back:
-- `docs/late-data-policy.md` LD-1 asks for the measured lag of this source,
-- and the delivery is the measurement.
SELECT DISTINCT event_time_utc::DATE AS ds
FROM copperline.raw.pay_meridian_settlements
WHERE loaded_at >= ?::DATE
  AND loaded_at < ?::DATE + INTERVAL 1 DAY
ORDER BY ds
