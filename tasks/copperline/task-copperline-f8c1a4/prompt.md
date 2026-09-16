# LF-1121 — the Monday settlement summary has stopped going out, and the run is red on the payout tie

Seller-ops raised this on Friday. They have had no settlement summary for weeks.
`payment_settlement_weekly` goes red on `tie_to_payouts` every Monday, so the
email never leaves, and seller-ops have been answering sellers off the payout
table by hand. That is not something we can keep doing:
`contracts/settlement-summary.md` SS-2 says the summary is what the mart holds.

The error is the same shape every week — the summary totals far less than the
payouts do, by a long way.

Commerce's first read is the one the step itself offers: the fee schedule the run
rated the week on is not the one the payouts were computed on, so the rates are
stale and the money comes out short. Test that before you accept it.

Find what is actually wrong and fix it. When you are done:

- a Monday run summarises a week — the week `contracts/settlement-summary.md`
  says it owns, all of it and none of any other week's days;
- `tie_to_payouts` passes on its own, on any Monday, because the two sides
  finally agree and not because the comparison was loosened or removed.

Two things stay as they are.

- The seven steps of `payment_settlement_weekly`. Every one keeps its id, its
  operator and its place in the chain. `tie_to_payouts` in particular is the last
  thing standing between a wrong summary and a seller, so it stays, and it stays
  a comparison.
- The figures. `contracts/settlement_weekly.yml` names the six columns, and how
  each one is computed is not in scope this week.

You will not get the whole DAG through on this box. `apply_fee_schedule` reads a
staging table the marketplace team's own job writes and it is not in the
warehouse copy here, so a run stops there whatever you do. That is a separate
ticket. The steps either side of it run on their own.

Then write it up in `RESPONSE.md` at the root of the working tree. Short —
commerce reads it in the channel, and seller-ops get the one-line version:

- what a scheduled Monday run actually covered, and why that window held nothing
- which days a Monday run should cover instead, and where that answer comes from
- whether the fee schedule is what broke the tie, and what ruled it in or out
