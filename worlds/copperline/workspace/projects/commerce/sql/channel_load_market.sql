-- One market's channel rows for the day, replaced.
INSERT OR REPLACE INTO copperline.staging.channel_market
WITH ask(ds, market_code) AS (SELECT CAST(? AS DATE), CAST(? AS VARCHAR))
SELECT ask.ds, ask.market_code, o.channel,
       count(*)                 AS orders,
       sum(o.grand_total_cents) AS booked_cents
FROM copperline.raw.orders o, ask
WHERE o.local_order_date = ask.ds
  AND o.market_code = ask.market_code
  AND NOT o.is_test
GROUP BY ask.ds, ask.market_code, o.channel
