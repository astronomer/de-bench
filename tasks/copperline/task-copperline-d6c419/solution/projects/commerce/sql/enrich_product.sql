-- The product attributes in force on the order date, from Palette's change
-- feed.
--
-- This used to read `marts.dim_product`. Three things were wrong with that.
-- The dimension is dbt's and is not in this warehouse until the analytics build
-- has run; `valid_from` and `valid_to` are on no version of it; and the version
-- dbt does build keeps one row per SKU carrying the record that SKU is on
-- today, so it could not answer for an order three months back even if it were
-- here. MER-612 is spanning the feed out into a dimension that can. Until it
-- lands, the record comes off the feed the dimension is built from, the way
-- `close_price_book` takes the close's price book off it.
--
-- ONE PARAMETER, BOUND ONCE AND READ TWICE. `nightly_close` runs this same
-- file through `build_step`, which passes the date and nothing else, so a
-- second placeholder would break the close.
--
-- THE ORDER DATE IS A DATE AND `updated_at` IS A STAMP. A change made at 13:47
-- is in force for the whole of the day it was made, so the comparison is
-- date to date. Held against the raw stamp, every change made during the order
-- day is missed.
--
-- A `delete` TAKES PART IN THE RANKING AND THEN LEAVES NOTHING BEHIND. Filter
-- the deletes out before the rank and the record the retirement closed stays in
-- force for ever, so a SKU the catalogue retired keeps colouring orders placed
-- after it went.
--
-- EVERY ORDER KEEPS ITS ROW AND EVERY LINE IS COUNTED. The order book carries a
-- far wider SKU range than the PIM does — `dim_product`'s docstring and
-- `fct_order_line`'s both say so — so the product join misses on most lines.
-- An inner join here drops most of the day's orders out of the enrichment
-- altogether, and `enrich_publish` left-joins this table, so nothing goes red.
CREATE OR REPLACE TABLE copperline.staging.enrich_product AS
WITH as_of AS (

    SELECT CAST(? AS DATE) AS order_date

),

ranked AS (

    SELECT v.sku,
           v.category_id,
           v.operation,
           row_number() OVER (
               PARTITION BY v.sku
               ORDER BY CAST(v.updated_at AS DATE) DESC,
                        CASE v.source
                            WHEN 'pim_ui' THEN 1
                            WHEN 'supplier_feed' THEN 2
                            WHEN 'bulk_load' THEN 3
                            ELSE 4
                        END,
                        v.updated_at DESC,
                        v.change_id
           ) AS version_seq
    FROM copperline.raw.pim_product_versions v
    CROSS JOIN as_of a
    WHERE CAST(v.updated_at AS DATE) <= a.order_date

),

in_force AS (

    SELECT sku, category_id
    FROM ranked
    WHERE version_seq = 1
      AND operation = 'upsert'

)

SELECT b.order_id,
       count(*)                       AS line_count,
       sum(l.qty)                     AS unit_count,
       count(DISTINCT p.category_id)  AS category_count
FROM copperline.staging.enrich_base b
JOIN copperline.raw.order_lines l ON l.order_id = b.order_id
LEFT JOIN in_force p ON p.sku = l.sku
CROSS JOIN as_of a
WHERE b.local_order_date = a.order_date
GROUP BY b.order_id
