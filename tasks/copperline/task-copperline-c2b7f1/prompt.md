# DQ-93 — the processor overlap tie, and the quarter underneath it

DQ-77 sorted the nightly's twelve failures into the stale and the real. DQ-91 closed the
marketplace one. This is the processor one, and payments have been waiting on it since the
migration signed off.

`tie_settlement_processor_overlap` goes red on every order the two processors both touched.
The DQ-77 write-up says the shadow quarter is the migration working as planned and that money
counted twice is not, and that the test cannot tell the two apart. Nobody has taken it further.
While it sits there we cannot answer the only question anyone asks about the migration, which
is how much of that quarter's money we have counted twice.

Read what `int_payment_matched` does with the two books. Then make it say what the migration
actually did, and rewrite the tie so it asserts something that holds.

Rules the answer keeps:

- **The window comes off the record that holds it, not off a date typed into a model.** Which
  book was authoritative on a day was decided twice during the migration and moved once, and it
  will move again when the Halcyon contract is closed out.
- **One row per order out of `int_payment_matched`**, as today. `grand_total_cents` is the
  order's own money and it does not move.
- **`settled_net_cents` keeps the arithmetic it has today.** The only thing that changes is
  which rows are inside it.
- **The two staging views stay 1:1 with the feeds.** They are what the processors sent us and
  other teams read them. Whatever you do about the overlap happens above them.
- **The overlap stays visible.** A model that quietly drops one side has hidden the one thing
  the reconciliation exists to find.
- **The test stays a test.** It runs green tonight and it goes red the moment a payment is
  counted twice. Green because it stopped asking anything is worse than the red we have. An
  order that settles for *less* than it was placed for is a refund, a reversal or an attempt
  the reference never matched — that is R-7's exception list and not this test's business.
- The recycled reference is PAY-233's ticket, not this one. Leave the order-to-attempt match as
  you find it.

`payments_recon_daily` cannot run on this box and is out of scope. `dbt/README.md` has the run
commands, and do not use `dbt build` on this tree.

Then write four lines in `RESPONSE.md` at the root of the working tree, for the DQ-77 thread:

- what the flag was asserting, and why every order both books touched broke the test
- what the variance it has been reporting all this time actually is
- which record settles who was authoritative, and what it says
- which other published columns read the two books the same way, by name

Four lines. It goes on the thread, not into a document.
