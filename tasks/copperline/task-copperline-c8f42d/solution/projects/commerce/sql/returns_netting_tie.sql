-- Every order day tonight's authorisations name, with the count the returns
-- feed carries for it and the count the table holds. Parameters: the delivery
-- date, twice.
--
-- The two agree or the day is short, which is the failure this table exists to
-- end: a day published once and never revisited is missing every return raised
-- against it since. `partition_rows` is there because the grain is one row per
-- day, and a day holding two rows is a build that appended where it should
-- have replaced.
WITH named AS (
    SELECT DISTINCT o.local_order_date AS ds
    FROM copperline.raw.returns r
    JOIN copperline.raw.orders o ON o.order_id = r.order_id
    WHERE r.initiated_at >= ?::DATE
      AND r.initiated_at < ?::DATE + INTERVAL 1 DAY
      AND NOT o.is_test
), feed AS (
    SELECT o.local_order_date AS ds, count(*) AS rmas
    FROM copperline.raw.returns r
    JOIN copperline.raw.orders o ON o.order_id = r.order_id
    JOIN named n ON n.ds = o.local_order_date
    WHERE NOT o.is_test
    GROUP BY 1
), built AS (
    SELECT m.ds,
           sum(m.rmas) AS rmas,
           count(*)    AS partition_rows
    FROM copperline.marts.returns_by_order_day m
    JOIN named n ON n.ds = m.ds
    GROUP BY 1
)
SELECT f.ds,
       f.rmas,
       coalesce(b.rmas, 0)           AS built_rmas,
       coalesce(b.partition_rows, 0) AS partition_rows
FROM feed f
LEFT JOIN built b ON b.ds = f.ds
ORDER BY f.ds
