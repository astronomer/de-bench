-- One order day, split by the channel that sold it. Parameters: the order day,
-- twice.
--
-- THE CHANNEL. `channel` comes off the ORDER, because the report answers what
-- a channel gave back on what it sold. `raw.returns.return_channel` is the
-- door the goods came back through — `mail`, `store` or `marketplace` — and
-- 14% of authorisations come back through a door that did not sell them, so
-- reading it as the selling channel moves one row in seven into the wrong
-- channel while the company total stays identical. The two columns are not the
-- same vocabulary either: the order book sells through `store`, `web`,
-- `marketplace` and `trade`, and no return ever comes back through `web`.
-- `int_return_linked` takes the channel off the line for this reason.
--
-- THE DOOR, once. `counter_credit_cents` is the one place the door belongs: it
-- is the store credit handed over a counter, whichever channel sold the goods,
-- which is exactly the amount a store's own reconciliation shows against
-- somebody else's sale.
--
-- THE JOIN. `order_line_id` is a line ordinal — `L-01`, `L-02` — and repeats
-- in every order, twelve distinct values over nine million lines. The staging
-- layer says so on the source (`_sources.yml`) and again on
-- `stg_sales__returns`, and `raw.order_lines` carries the pair as its primary
-- key. A join on the ordinal alone matches every order that ever had a line in
-- that position.
--
-- THE WHOLE DAY. Every authorisation the feed carries against the day, however
-- long ago it was raised: `include/lib/warehouse.py` says a partition write
-- takes the whole partition, and `docs/late-data-policy.md` LD-2 says a day
-- that has moved is rebuilt rather than appended to.
--
-- `NOT is_test` is the population every commerce build works from —
-- `enrich_base`, `economics_tie`, `channel_load_market`, `stg_sales__orders`.
-- Orders the OMS soft-deleted stay in: `docs/reconciliation-policy.md` R-6
-- says a row the source deleted is a row that still has to be accounted for.
SELECT ?::DATE                                              AS ds,
       o.channel                                            AS channel,
       count(*)                                             AS rmas,
       sum(r.qty)                                           AS returned_qty,
       sum(r.refund_cents)                                  AS refund_cents,
       coalesce(sum(r.refund_cents)
                FILTER (r.refund_method = 'original'), 0)   AS card_refund_cents,
       coalesce(sum(r.refund_cents)
                FILTER (r.refund_method = 'store_credit'), 0)
                                                            AS store_credit_cents,
       coalesce(sum(r.refund_cents)
                FILTER (r.refund_method = 'store_credit'
                        AND r.return_channel = 'store'), 0) AS counter_credit_cents,
       sum(l.line_total_cents)                              AS returned_line_cents
FROM copperline.raw.returns r
JOIN copperline.raw.orders o
  ON o.order_id = r.order_id
JOIN copperline.raw.order_lines l
  ON l.order_id = r.order_id
 AND l.order_line_id = r.order_line_id
WHERE o.local_order_date = ?::DATE
  AND NOT o.is_test
GROUP BY 1, 2
ORDER BY 2
