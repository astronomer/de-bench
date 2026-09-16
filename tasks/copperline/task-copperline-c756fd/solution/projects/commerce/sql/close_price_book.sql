-- The price book in force on the close date, from the PIM change feed.
--
-- This used to read `marts.dim_product`. Three things were wrong with that.
-- The dimension is dbt's and is not in this warehouse until the analytics build
-- has run; `valid_from` and `valid_to` are on no version of it; and the version
-- it does build keeps one row per SKU carrying the price that SKU is on today,
-- so it could not price a night three months back even if it were here.
--
-- So the book comes off the feed the dimension is built from. The rule for
-- picking one version of a SKU is `snapshots/snap_product_price.sql`'s, spelled
-- there and again in `models/shared/staging/stg_product__pim_versions.sql`:
-- `upsert` rows only, the latest `updated_at`, ties broken pim_ui, then
-- supplier_feed, then bulk_load, then `change_id`. The only thing added here is
-- the close date, and a change stamped on the close date is in force for it.
CREATE OR REPLACE TABLE copperline.staging.close_price_book AS
WITH ranked AS (
    SELECT sku,
           list_price_cents,
           updated_at,
           row_number() OVER (
               PARTITION BY sku
               ORDER BY updated_at DESC,
                        CASE source
                            WHEN 'pim_ui' THEN 1
                            WHEN 'supplier_feed' THEN 2
                            WHEN 'bulk_load' THEN 3
                            ELSE 4
                        END,
                        change_id
           ) AS version_seq
    FROM copperline.raw.pim_product_versions
    WHERE operation = 'upsert'
      AND CAST(updated_at AS DATE) <= CAST(? AS DATE)
)
SELECT sku, list_price_cents, updated_at AS priced_from
FROM ranked
WHERE version_seq = 1
