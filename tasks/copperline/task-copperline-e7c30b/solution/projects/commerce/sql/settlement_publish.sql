-- The week's summary rows, one per seller. `publish_week` writes them through
-- `warehouse.delete_insert`, keyed on `week_start`, so a run replaces the week
-- it owns and leaves every other week alone.
--
-- This statement used to be a bare `INSERT INTO copperline.marts.settlement_weekly`
-- with nothing to remove what was already there, which is the defect
-- `CONVENTIONS.md`, "Writing to the warehouse", names: the second run of an
-- interval puts the rows in twice.
SELECT CAST(? AS DATE) AS week_start,
       seller_id,
       sum(CASE WHEN line_type = 'principal' THEN -amount_cents ELSE 0 END)
           AS gmv_cents,
       sum(CASE WHEN line_type = 'commission' THEN amount_cents ELSE 0 END)
           AS commission_cents,
       sum(CASE WHEN line_type = 'fulfilment_fee' THEN amount_cents ELSE 0 END)
           AS fee_cents,
       sum(-amount_cents) AS payout_cents
FROM copperline.staging.settlement_lines
WHERE payout_date >= ?
GROUP BY seller_id
