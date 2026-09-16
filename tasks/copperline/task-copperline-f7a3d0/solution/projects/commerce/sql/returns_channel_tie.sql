-- One order day, the feed's count against the table's, channel by channel.
-- Parameters: the order day, twice.
--
-- A full outer join, so a channel the build dropped and a channel the build
-- invented are both visible. A `mail` row on the built side is the whole point:
-- the door is not the selling channel, and a build that read it as one shows up
-- here as a channel the order book never sold through.
WITH feed AS (
    SELECT o.channel        AS channel,
           count(*)         AS rmas
    FROM copperline.raw.returns r
    JOIN copperline.raw.orders o ON o.order_id = r.order_id
    WHERE o.local_order_date = ?::DATE
      AND NOT o.is_test
    GROUP BY 1
),
built AS (
    SELECT channel      AS channel,
           sum(rmas)    AS rmas,
           count(*)     AS rows_written
    FROM copperline.marts.returns_by_channel_day
    WHERE ds = ?::DATE
    GROUP BY 1
)
SELECT coalesce(f.channel, b.channel)   AS channel,
       coalesce(f.rmas, 0)              AS feed_rmas,
       coalesce(b.rmas, 0)              AS built_rmas,
       coalesce(b.rows_written, 0)      AS rows_written
FROM feed f
FULL OUTER JOIN built b ON b.channel = f.channel
ORDER BY 1
