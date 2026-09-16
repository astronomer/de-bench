# LF-1103 — the summary keeps the credits we hand back

Seller-ops have been arguing with a marketplace seller since Tuesday and I
think the seller is right.

The seller took a return in April and the credit for it is on their
remittance. It does not show on the weekly settlement summary the way anybody
expects. The week they went looking at has not moved a cent. A week they were
sent back in March has: the held-back figure on it is larger than the copy
that went out, by roughly the credit. Seller-ops pulled four more sellers who
had a return against them and got the same shape every time. A seller-week
with no return against it looks right.

The summary is `marts.settlement_weekly` and `contracts/settlement-summary.md`
SS-1 says what belongs in it. `commission_cents` and `fee_cents` together are
what we held back out of the seller's orders before the payout left. They are
not holding back what we actually kept.

Work out what the fourth line type on the marketplace settlement feed is
telling us, and make those two columns say what we kept.

Four things stay as they are.

- **`gmv_cents` does not move.** It is the seller's order value and
  `docs/finance-policy.md` REV-14 owns it.
- **The grain stays.** One row per seller per fiscal week, per SS-1.
- **The week a row is cut on does not move.** Re-cutting the summary on a
  different date is a separate ticket and nobody has raised it.
- **`payout_cents` is not yours this week, and neither is
  `int_settlement_matched`.** DQ-91 has that model open. The payout column
  reads one of these same lines the wrong way round and the fix goes in there.
  Leave both alone.

`payment_settlement_weekly` is not in scope either. LF-1042 has that tree open
and its fee-schedule step still cannot run on this box, so the table that is
actually in the warehouse is the one dbt builds.

Then write seller-ops three lines in `RESPONSE.md` at the root of the working
tree. It goes on the thread, not into a document.

- what the fourth line type is, and which way it moves what we kept
- how far out the held-back figure has been on a week that holds one
- which week a credit lands in, and why a week that went out in March can
  still move in April
