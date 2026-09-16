CREATE OR REPLACE TABLE copperline.staging.cart_abandon AS
SELECT f.ds, f.market_code,
       sum(f.carts_started)                         AS carts_started,
       sum(f.carts_started) - sum(f.carts_ordered)  AS carts_abandoned
FROM copperline.marts.funnel_daily f
WHERE f.ds = ?
GROUP BY f.ds, f.market_code
