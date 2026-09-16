# DQ-91 — the marketplace tie

- **What the tie asserted.** That an order's `gmv_cents` equals the principal plus the
  commission plus the fulfilment fee. Every settled order broke it, because the commission and
  the fulfilment fee are our take: they are held back out of the order, so the tie added them
  where it should have taken them off, and overstates each settled order by them.
- **What the variance was.** Our own revenue. On each order the gap the tie reported is exactly
  that order's commission plus its fulfilment fee, to the cent. There is no order in the feed
  where the two sides disagree about the seller's money.
- **What the principal line is.** The gross order value, owed to the seller before anything is
  held back. It equals `gmv_cents` on every settled order, and it signs negative because the
  money leaves us. It is not the seller's net, which is what
  `int_settlement_matched.seller_net_cents` read it as.
- **Who else read it that way.** `marts.gmv_daily.seller_net_cents` and
  `marts.settlement_weekly.payout_cents` both sum that column, so both have been reporting the
  gross order value as the payout. Both come right once the model does. The same pivot also put
  every refunded order in the model twice, because the refund posts with a later payout run
  than the commission it reverses.
