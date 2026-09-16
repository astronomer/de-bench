{#-
    Tie: store sales in the order book against the till.

    The order book says what sold through the store channel on a day. The POS
    batch says what the till took. They are the same money and they should
    agree.

    A tolerance of one hundred cents a store-day is allowed for the rounding in
    split tenders. Traceable to contracts/daily-flash.md, which commits the
    flash to a single store sales number.
-#}

select
    s.ds,
    s.store_id,
    s.net_sales_cents,
    s.till_net_cents,
    s.variance_cents,
    s.late_batches,
    s.missing_batches
from {{ ref('agg_daily_store_sales') }} s
where s.order_count > 0
  and s.till_txn_count > 0
  and abs(s.variance_cents) > 100
