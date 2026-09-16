-- The buyers' SKU page. New with MER-311; nothing else creates it.
-- Qualified, because `warehouse.connect` puts the read-only `nwv` attach first
-- on the search path and an unqualified name lands there.
CREATE TABLE IF NOT EXISTS copperline.marts.product_margin_daily (
    ds                      DATE,
    sku                     VARCHAR,
    line_count              BIGINT,
    gross_line_cents        BIGINT,
    header_discount_cents   BIGINT,
    discounted_line_cents   BIGINT,
    landed_cost_cents       BIGINT,
    line_margin_cents       BIGINT
)
