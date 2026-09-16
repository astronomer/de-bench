-- The product history the close and the enrichment read: one row per SKU per
-- span of time in which its record did not change, rebuilt whole from Palette's
-- change feed.
--
-- ORDERED ON `updated_at`, NEVER ON `received_at`. The feed arrives out of
-- order with no bound (`docs/late-data-policy.md`), so a change that landed a
-- week after the change that superseded it still belongs where its own stamp
-- puts it. Ordering on arrival appends it to the end of the history instead of
-- splicing it into the middle, and every as-of read after that day is wrong.
--
-- A DAY CARRIES ONE ROW PER SKU. Where two changes share a date the source
-- decides -- pim_ui, then supplier_feed, then bulk_load -- and where they came
-- from the same source the later stamp does. Without the rank the loser opens a
-- span the winner closes on the same date, which no reader can ever match.
--
-- A `delete` CLOSES A SPAN AND OPENS NOTHING. It has to take part in the
-- ordering to close anything, so it is dropped after the span ends are
-- computed and not before: filter it out first and the span in force runs on
-- to the next upsert, which swallows the retirement and the gap a resurrected
-- SKU leaves behind it.
CREATE OR REPLACE TABLE copperline.marts.dim_product AS
WITH ranked AS (

    SELECT *,
           CAST(updated_at AS DATE) AS change_date,
           row_number() OVER (
               PARTITION BY sku, CAST(updated_at AS DATE)
               ORDER BY CASE source
                            WHEN 'pim_ui' THEN 1
                            WHEN 'supplier_feed' THEN 2
                            WHEN 'bulk_load' THEN 3
                            ELSE 4
                        END,
                        updated_at DESC,
                        change_id
           ) AS same_day_seq
    FROM copperline.raw.pim_product_versions

),

spans AS (

    SELECT sku,
           product_name,
           category_id,
           brand,
           supplier_id,
           list_price_cents,
           status,
           operation,
           change_date AS valid_from,
           lead(change_date) OVER (PARTITION BY sku ORDER BY change_date)
               AS valid_to
    FROM ranked
    WHERE same_day_seq = 1

)

SELECT sku,
       product_name,
       category_id,
       brand,
       supplier_id,
       list_price_cents,
       status,
       valid_from,
       valid_to
FROM spans
WHERE operation = 'upsert'
