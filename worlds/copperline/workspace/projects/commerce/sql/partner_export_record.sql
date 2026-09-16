INSERT OR REPLACE INTO copperline.ops.partner_exports
SELECT ?::DATE AS ds, 'order_economics' AS subject, count(*) AS rows_out
FROM copperline.marts.order_economics
WHERE order_date = ?::DATE
