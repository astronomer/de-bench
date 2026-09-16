-- Variants with more than one open snapshot row, and the open row count.
SELECT
    (SELECT count(*) FROM (
        SELECT sku FROM copperline.marts.dim_product
         WHERE valid_to IS NULL AND valid_from <= ?
         GROUP BY sku HAVING count(*) > 1)),
    (SELECT count(*) FROM copperline.marts.dim_product WHERE valid_to IS NULL)
