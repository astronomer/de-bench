# DQ-93 — the processor overlap tie

**What the flag was asserting.** `is_shadow_quarter` in `int_payment_matched` means "both books
hold this order" and has no date in it at all. Both books held every payment in the migration
quarter, on purpose, so the flag is true of the whole overlap by construction and the tie
selected all of it. It was never asking whether the shadow quarter was over; it was asking
whether the shadow quarter happened.

**What the variance is.** The model adds Meridian's captures to Halcyon's settlements, so for
every order both books claim, `meridian_captured_cents + halcyon_settled_cents` is the same
payment counted twice and the variance the tie reports is the order's own total. The two feeds
agree to the cent on the payments they share, so none of that number is a disagreement about
money — it is one payment added to itself.

**What settles it.** `raw.pay_processor_windows`: Halcyon is authoritative through 2025-08-31
and Meridian from 2025-09-01. The boundary is in the middle of the overlap, not at the end of
it — the tie's header assumed 2025-09-30, which is when the second feed stopped, not when the
authority moved — and it applies to a settlement row on the row's own date rather than to an
order on the order's date. A payment captured in August and settled in September is claimed by
both books and both are right, each on its own date; a few dozen orders split that way and their
two authoritative halves add to the order exactly. `int_payment_matched` now takes each row from the
book whose window holds its date, keeps what the other book claimed in `shadow_cents`, and
flags `is_shadow_quarter` off that rather than off "both books hold it".

**Who else reads the two books this way.** In commerce, everything netted off this model:
`marts.fct_payments.settled_net_cents` and `unsettled_cents`, `marts.fct_order.settled_net_cents`,
and the `shadow_quarter` rows of `marts.recon_exceptions`, which reports the doubled figure as
the variance. In finance, the ledger: `int_gl_postings_unified` posts both card books whole into
`marts.fct_gl_postings`, so the migration quarter's cash is in there twice, and
`marts.cash_recon_daily` nets it with a day-level `least(meridian, halcyon)` and says in its own
header that it can report the overlap and not resolve it. The ledger is platform's model and
this ticket does not touch it; it now has a window table to read and somebody should raise it.
