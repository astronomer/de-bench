SELECT count(*) FROM copperline.marts.refunds_daily
WHERE ds = ? AND rev6_side IS NULL
