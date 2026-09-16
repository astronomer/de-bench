-- Channel and market attributes. Four channels: store, web, marketplace, trade.
CREATE OR REPLACE TABLE copperline.staging.enrich_channel AS
SELECT b.order_id, b.channel, b.market_code,
       c.is_trading_day, c.holiday_name
FROM copperline.staging.enrich_base b
LEFT JOIN copperline.raw.market_calendar c
       ON c.market_code = b.market_code
      AND c.calendar_date = b.local_order_date
WHERE b.local_order_date = ?
