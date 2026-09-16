INSERT INTO copperline.marts.cart_abandon_daily BY NAME
SELECT ds, market_code, carts_started, carts_abandoned
FROM copperline.staging.cart_abandon
WHERE ds = ?
