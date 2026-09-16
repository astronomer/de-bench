# PRO-1140 — why the promotion page is short

The short number is `dim_promotion.discount_cents`, and it comes through
`int_promo_exposure`. That model starts from eligibility: it joins the
promotion register to the orders each promotion says it covers — inside the
promotion's window, in one of its markets, at or above its minimum order — and
then attaches the applications to that join. An application the register cannot
account for is not on the join, so it is dropped, and its money is dropped with
it.

It is short, never long. Every cent it drops is a cent the OMS recorded against
a real order, so the money is real and we gave it away. `marts.promo_cost_daily`
now carries it: the promotion feed, the whole of it, tied to the order-level
and line-level discounts on the same orders.

The exposure model is not wrong for what it was built for. Take-up is a
question about eligibility and cost is not, and the two have been reading the
same column.
