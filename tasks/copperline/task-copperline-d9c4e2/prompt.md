# PLAT-471 — the ledger books the migration quarter's card cash twice

Finance is restating the FY2025 comparatives and has stopped on Q3. Card cash in the ledger
for July and August 2025 is about twice June's, and September's sits a third above
October's. Nothing in the order book for those months moved — the count and the value are
flat from May to October.

`marts.cash_recon_daily` is the model that should have caught this. It marks every day of
that quarter as a shadow-quarter day, it reports an overlap figure, and the cash it reads
off the ledger agrees with what it reads off the processor feeds. To the cent. Every day of
the quarter, and every day since.

`int_gl_postings_unified` is the card book and platform owns it. Make it post the money that
was ours to post, and make the recon mart report the rest instead of adding it in.

Rules the answer keeps:

- **The window comes off the record that holds it, not off a date typed into a model.**
  Which book was authoritative on a day was decided twice during the migration and moved
  once, and it will move again when the Halcyon contract is closed out. That record is a
  landed table no model reads yet, and rule 1 in `projects/platform/README.md` says what a
  model has to do before it can reach one.
- **A settlement row is decided on its own date.** Not on the payment's, and not on the
  order's. The awkward case is a payment one book took in August and the other settled in
  September, and both of those are real.
- **Both legs of a card posting go together.** A card row debits cash and credits clearing.
  A document that does not sum to zero is not a posting, it is a plug.
- **Nothing outside the card book moves.** The other four subledgers post exactly what they
  post today.
- **The two staging views stay 1:1 with the feeds.** They are what the processors sent us,
  other teams read them, and commerce has its own ticket open on the same quarter. Whatever
  you do about the overlap happens above them.
- **The overlap stays reported.** `marts.cash_recon_daily` is where it is reported, and after
  the change it means this:
  - `meridian_cents` and `halcyon_cents` are what each processor settled on the days it was
    authoritative for. They no longer both carry money on the same day.
  - `shadow_overlap_cents` is what the other book sent for that day — the money the ledger
    does not post any more — signed the way the two columns above are signed.
  - the mart keeps its grain, one row per day, entity and currency, and those three figures
    stay keyed on the day alone.
- **`int_payment_matched` and the tie that reads it are DQ-93's.** Payments are working that
  one. Leave the commerce tree as you find it.

`payments_recon_daily` and `fin_cash_recon_daily` cannot run on this box and are out of
scope. `dbt/README.md` has the run commands, and do not use `dbt build` on this tree.
