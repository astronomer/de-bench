# LF-1103 — the weekly settlement summary

- **What the fourth line is.** `refund`. It is the credit we raise when a
  marketplace order comes back, it is minted as a reversal of the commission we
  charged, and the feed signs it negative because the money goes back out to the
  seller. So it comes off what we kept, not on to it. The summary was adding it.
- **How far out.** Twice the credit, on any seller-week that holds one. Adding
  it instead of taking it off puts it on the wrong side of the sum, so the
  held-back pair on that week is over by two credits and the payout the seller
  is quoted from it is short by the same. A week with no return against it is
  right to the cent.
- **Which week it lands in.** The week the order was placed, not the week the
  credit posted. The credit rides a payout run three weeks after the one that
  carried the commission, and the summary is cut on the order's own week — so a
  credit raised in April moves a week that was sent out in March, and the mart
  is rebuilt whole every night, so the copy in the warehouse stops agreeing with
  the copy seller-ops sent.
