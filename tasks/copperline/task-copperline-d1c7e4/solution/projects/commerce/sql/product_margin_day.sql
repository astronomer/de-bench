-- One day of the buyers' SKU page, from the landed order feed.
--
-- The header discount is on the order and belongs to no line, so it is spread
-- across the lines here the way `int_order_lines_discounted` spreads it, and
-- for the reasons that model gives: pro rata by tax-exclusive line total, in
-- whole cents, largest remainder, and nothing allocated against an order whose
-- lines sum to zero. Same rule, same cent, so the page and the shared models
-- agree.
--
-- The population is the order spine's: `stg_sales__orders` drops test orders
-- and rows the OMS deleted, and drops nothing else.
WITH live AS (

    SELECT order_id,
           order_discount_cents
    FROM copperline.raw.orders
    WHERE local_order_date = ?
      AND NOT coalesce(is_test, false)
      AND deleted_at IS NULL

),

joined AS (

    SELECT l.order_id,
           l.line_no,
           l.sku,
           l.qty,
           l.unit_cost_cents,
           l.line_total_cents,
           o.order_discount_cents,
           sum(l.line_total_cents) OVER (PARTITION BY l.order_id)
                                                   AS order_line_total_cents
    FROM copperline.raw.order_lines l
    JOIN live o ON o.order_id = l.order_id

),

floors AS (

    SELECT *,
           CASE
               WHEN order_line_total_cents > 0
               THEN cast(
                   (cast(order_discount_cents AS DECIMAL(38, 6)) * line_total_cents)
                   / order_line_total_cents
                   AS DECIMAL(38, 6))
               ELSE cast(0 AS DECIMAL(38, 6))
           END                                     AS exact_share
    FROM joined

),

remainders AS (

    SELECT *,
           cast(floor(exact_share) AS BIGINT)      AS floor_cents,
           order_discount_cents
               - sum(cast(floor(exact_share) AS BIGINT)) OVER (PARTITION BY order_id)
                                                   AS cents_to_hand_out,
           row_number() OVER (
               PARTITION BY order_id
               ORDER BY exact_share - floor(exact_share) DESC,
                        line_total_cents DESC,
                        line_no
           )                                       AS remainder_rank
    FROM floors

),

allocated AS (

    SELECT sku,
           line_total_cents,
           CASE
               WHEN remainder_rank <= cents_to_hand_out THEN floor_cents + 1
               ELSE floor_cents
           END                                     AS header_discount_cents,

           -- qty is a decimal upstream because a trade line can be a part
           -- quantity. The extension rounds half-up to the cent, once, and
           -- stays an integer — `int_order_lines_costed` does the same.
           cast(round(cast(qty AS DECIMAL(18, 4)) * unit_cost_cents, 0) AS BIGINT)
                                                   AS extended_cost_cents
    FROM remainders

)

SELECT sku,
       count(*)::BIGINT                                    AS line_count,
       sum(line_total_cents)::BIGINT                       AS gross_line_cents,
       sum(header_discount_cents)::BIGINT                  AS header_discount_cents,
       (sum(line_total_cents) - sum(header_discount_cents))::BIGINT
                                                           AS discounted_line_cents,
       sum(extended_cost_cents)::BIGINT                    AS landed_cost_cents,
       (sum(line_total_cents) - sum(header_discount_cents)
        - sum(extended_cost_cents))::BIGINT                AS line_margin_cents
FROM allocated
GROUP BY sku
ORDER BY sku
