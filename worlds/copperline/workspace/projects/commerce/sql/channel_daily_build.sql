-- The markets that landed, rolled up for the day.
SELECT CAST(? AS DATE) AS ds, market_code, channel,
       sum(orders)       AS orders,
       sum(booked_cents) AS booked_cents
FROM copperline.staging.channel_market
WHERE ds = ?
GROUP BY market_code, channel
ORDER BY market_code, channel
