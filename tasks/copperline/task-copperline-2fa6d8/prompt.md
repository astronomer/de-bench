# DQ-91 — the marketplace tie DQ-77 left red

DQ-77 sorted the nightly's twelve failures into two piles. Nine asserted a rule we no longer
hold and were retired. Three had found something and were left alone. This ticket is the
marketplace one, and I want it closed first, because it is the one nobody can explain.

`tie_gmv_settlement_reconstructs_order` does not fail on a handful of orders. It fails on every
settled marketplace order there is. Two quarters of that and not one person on the rotation has
produced an order where the two sides genuinely disagree about money.

There is a second thing on the same model and I think it is the same thing. Seller-ops quoted
`seller_net_cents` out of `int_settlement_matched` to a seller in March, the seller said that is
not what arrived, and seller-ops have not quoted the column since. The payout feed is the record
of what actually left our account and nobody has ever held the two up against each other.

Work out what the settlement feed is really telling us. Then make `int_settlement_matched` say
it, and rewrite the tie so it asserts the reconstruction that holds. Four rules the answer
keeps:

- **The tolerance stays where it is.** `contracts/settlement-summary.md` put it there. A
  consumer contract is not a dial and neither is the severity on a test.
- **The test stays a test.** After tonight it has to go red if a settlement line goes missing,
  lands on the wrong order or comes back restated. Green because it stopped asking anything is
  worse than the red we have.
- **One row per marketplace order out of `int_settlement_matched`**, settled or not. A seller's
  week must not be able to count the same order twice.
- **`gmv_cents` does not move.** It is the seller's order value, `docs/finance-policy.md` REV-14
  owns it, and whatever the settlement lines say, they do not get to redefine it.

The settlement DAG is not in scope. LF-1042 has that tree open and the fee-schedule step still
cannot run on this box.

Then write four lines in `RESPONSE.md` at the root of the working tree, for the DQ-77 thread:

- what the tie was asserting, and why every settled order broke it
- what the variance it has been reporting all this time actually is
- what the settlement feed's principal line is, in one sentence
- which other published columns were reading that line the same way, by name

Four lines. It goes on the thread, not into a document.
