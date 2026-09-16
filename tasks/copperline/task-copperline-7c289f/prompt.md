# CUS-517 — Compass has never scored an account from the acquired book

Growth want a first-order campaign at the accounts that came over with
Northwave. They asked the modelling team for Compass scores and were told there
are none, for any of those accounts, on any day.

The modelling team went through a month of the feature files by hand. Not one
`NWA-#####` id is in any of them. Every file holds about four thousand rows and
has done since the export started.

NW-214 put the company at 5,040 active trade accounts over the two books three
weeks ago, and planning is using that number. The feature files are counting
one book.

Nothing has failed. `check_as_of` is green, `check_contract` passes, and the
grain test has never fired.

`contracts/feature-store.md` binds this. Consumer C-9 in
`docs/report-registry.md` says who publishes it.

## What to do

The file covers our trade accounts. Both books. Make it do that.

Four things hold.

- **One row per account per day.** FS-2. An account is on a day once, or it is
  not on that day at all.
- **An acquired account keeps the id the acquired book knows it by.** The
  modelling team join scores back on `NWA-#####` and so does the CRM. Nothing
  here re-keys an account onto another id.
- **An account somebody has decided is the same customer as a Copperline
  account is one account, not two.** `ops.merge_candidates` holds those
  decisions, the customer master already applies them, and NW-214 settled what
  counts as a decision. Nothing here reopens it.
- **The attributes are the ones the customer master already carries for an
  account.** This ticket is about which accounts are on the file. It does not
  restate an attribute and it does not derive one a second way.

Two more, so this does not collide with the work already open on the table.

- **No window moves and no column's arithmetic moves.** The trailing windows,
  their bounds, and every figure the accounts already on the file carry come
  out exactly as they do today. CUS-509 is on the netting and this is not that.
- **The shared layer stays as it is.** Four teams read the staging models and
  the shared intermediate models, and the customer dimension is under a
  platform contract that C-8 depends on. Whatever this needs, it needs at the
  table that is short of rows.
