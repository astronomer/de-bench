-- The price book in force on the close date, from the snapshot.
CREATE OR REPLACE TABLE copperline.staging.close_price_book AS
SELECT sku, list_price_cents, valid_from, valid_to
FROM copperline.marts.dim_product
WHERE valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
