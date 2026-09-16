# LF-1042 — the settlement summary came back double for the week we replayed

Seller-ops have three sellers on the phone this morning. The Monday settlement
summary they were sent after the corrected fee schedule landed lists every
seller twice, and the payout figure on it is twice what those sellers were
actually paid. It is the week seller-ops asked us to re-run on 9 February.
Every other week they have pulled looks right.

The re-run was the right thing to do. `contracts/settlement-summary.md` calls a
replay after a fee-schedule change a normal request. The rates that came back
were right. What is not normal is the week coming back with two of everything.

Commerce's first read is that this is the two-feed overlap — both processors
sent settlements for the same money, and the summary is summing both sides.
Test that before you accept it.

Find what actually doubles the week and fix it. After the fix, re-running a week
that has already gone out replaces that week, and no other week moves. A replay
still has to write: seller-ops replay a week precisely to correct the figures on
it, so a step that sees the week already there and leaves it alone is no better
than what we have.

Two things stay as they are.

- The figures. `contracts/settlement_weekly.yml` names the six columns, and how
  each one is computed is not in scope this week.
- The seven steps of `payment_settlement_weekly`. Every one keeps its id, its
  operator and its place in the chain.

You will not get the whole DAG through on this box. `apply_fee_schedule` reads a
staging table the marketplace team's own job writes and it is not in the
warehouse copy here, so a run stops there whatever you do. That is a separate
ticket. The publish step runs on its own.

Then write it up in `RESPONSE.md` at the root of the working tree. Short —
commerce reads it in the channel, seller-ops get the one-line version:

- which statement doubles the week, named
- which week the seven dates seller-ops re-ran actually land on, and why those
  seven dates are one week rather than seven
- whether the two-feed overlap is the cause, and what ruled it in or out
- whether anything else in commerce publishes the same way
