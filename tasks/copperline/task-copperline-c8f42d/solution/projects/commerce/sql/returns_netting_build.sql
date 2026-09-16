-- One order day, whole. Parameters: the order day, twice.
--
-- THE JOIN. `order_line_id` is a line ordinal — `L-01`, `L-02` — and repeats
-- in every order, twelve distinct values over nine million lines. The staging
-- layer says so on the source (`_sources.yml`) and again on
-- `stg_sales__returns`. The line an authorisation names is
-- (`order_id`, `order_line_id`), and a join on the ordinal alone matches every
-- order that ever had a line in that position.
--
-- The whole day is selected, not the part of it this night delivered:
-- `include/lib/warehouse.py` says a partition write takes the whole partition,
-- and a day rebuilt from tonight's authorisations alone has lost the rest.
--
-- `NOT is_test` is the population every commerce build works from —
-- `enrich_base`, `economics_tie`, `channel_load_market`, `stg_sales__orders`.
-- Orders the OMS soft-deleted stay in: `docs/reconciliation-policy.md` R-6
-- says a row the source deleted is a row that still has to be accounted for.
SELECT ?::DATE                                              AS ds,
       count(*)                                             AS rmas,
       count(*) FILTER (r.received_at IS NULL)              AS open_rmas,
       sum(r.qty)                                           AS returned_qty,
       sum(r.refund_cents)                                  AS refund_cents,
       coalesce(sum(r.refund_cents)
                FILTER (r.refund_method = 'original'), 0)   AS card_refund_cents,
       sum(l.line_total_cents)                              AS returned_line_cents
FROM copperline.raw.returns r
JOIN copperline.raw.orders o
  ON o.order_id = r.order_id
JOIN copperline.raw.order_lines l
  ON l.order_id = r.order_id
 AND l.order_line_id = r.order_line_id
WHERE o.local_order_date = ?::DATE
  AND NOT o.is_test
GROUP BY 1
