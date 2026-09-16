-- The order days one night's authorisations name, oldest first. Parameters:
-- the delivery date, twice.
--
-- An authorisation is raised between three and sixty days after the order it
-- came off, so a single night names order days spread over two months. Those
-- are the days this run owes a rebuild.
--
-- Taken from the delivery rather than from a fixed number of days back:
-- `docs/late-data-policy.md` LD-1 asks for the measured reach of the source
-- and says not to copy a window from another job, and the night's own rows are
-- the measurement. Thirty days — the habit — leaves half the days untouched.
--
-- `is_test` rows are out here for the same reason they are out of the figures:
-- a day named only by a staff transaction is a day with nothing to publish.
SELECT DISTINCT o.local_order_date AS ds
FROM copperline.raw.returns r
JOIN copperline.raw.orders o ON o.order_id = r.order_id
WHERE r.initiated_at >= ?::DATE
  AND r.initiated_at < ?::DATE + INTERVAL 1 DAY
  AND NOT o.is_test
ORDER BY ds
