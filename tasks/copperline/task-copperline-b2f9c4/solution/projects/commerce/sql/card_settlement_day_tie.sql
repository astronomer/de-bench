-- Every settlement day tonight's delivery touched, with the events the feed
-- carries for it as at tonight and the events the table holds for it.
-- Parameters: the delivery date, three times.
--
-- The two counts agree or the day is short, which is the failure this table
-- exists to end: a day published once and never revisited is missing whatever
-- arrived after it was published.
WITH touched AS (
    SELECT DISTINCT event_time_utc::DATE AS ds
    FROM copperline.raw.pay_meridian_settlements
    WHERE loaded_at >= ?::DATE
      AND loaded_at < ?::DATE + INTERVAL 1 DAY
), feed AS (
    SELECT s.event_time_utc::DATE AS ds, count(*) AS events
    FROM copperline.raw.pay_meridian_settlements s
    JOIN touched t ON t.ds = s.event_time_utc::DATE
    WHERE s.loaded_at < ?::DATE + INTERVAL 1 DAY
    GROUP BY 1
), built AS (
    SELECT m.ds,
           sum(m.events)                   AS events,
           count(*)                        AS partition_rows,
           count(DISTINCT m.currency_code) AS currencies
    FROM copperline.marts.card_settlement_daily m
    JOIN touched t ON t.ds = m.ds
    GROUP BY 1
)
SELECT f.ds,
       f.events,
       coalesce(b.events, 0)          AS built_events,
       coalesce(b.partition_rows, 0)  AS partition_rows,
       coalesce(b.currencies, 0)      AS currencies
FROM feed f
LEFT JOIN built b ON b.ds = f.ds
ORDER BY f.ds
