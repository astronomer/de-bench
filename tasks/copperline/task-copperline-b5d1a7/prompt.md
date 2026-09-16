# PAY-276 — the same order, a different payment on Tuesday

Recovery ops rebuild the refund worksheet off `marts.fct_payments` every morning. They rebuilt
Monday's on Tuesday to add a column and nine of the rows came back naming a different attempt
than Monday's copy did. Nothing loaded in between — same orders, same feed, same day.

It is not nine rows. Diff two nights of `fct_payments` and tens of thousands of orders carry a
different `payment_intent_id`, and `settled_net_cents` moves with them. Finance signs a cash
number off that column on the first of the month and it has to be the same number if we run it
again on the second.

The order-to-attempt match lives in `int_orders_enriched`. Read what it does when an order has
more than one attempt inside its window.

While you are in there. The payments squad has been told for a year to look at
`payment_match_is_ambiguous` before trusting `payment_outcome`, and the model's own note says
that flag is true when more than one order shares the reference in the window. It is true on
better than a fifth of the matched book, and nobody has ever managed to act on one. Payments
want to know whether the flag earns the exception queue it feeds. Work out what it is actually
saying and how much of the estate really is ambiguous the way the note describes.

## The fix

Make the pick repeatable.

- **The ranking we have is the ranking we want.** Nearest by day, a success ahead of a failure,
  then the lower attempt number, over the attempts inside the seven-day window either side of
  the order. An order whose ranking already produces one clear winner keeps the attempt it has
  today. Do not reorder those three and do not move the window.
- **Where the three still leave two attempts level**, we cannot tell which of them is the
  order's own and the worksheet has to stop moving anyway. Take the one raised first — the
  earlier `created_at` — and where two ever carry the same instant, the lower `intent_id`.
- **One row per live order**, as today, and every column the model publishes keeps its name and
  its meaning. Half the estate reads this model.
- **The staging views stay 1:1 with what landed.** They are what the processor sent us and
  other teams read them; whatever you do about the duplicate attempts happens above them.
- **`payment_match_is_ambiguous` stays as it is.** Whatever it turns out to be saying, models
  we do not own read it, and re-cutting it is its own ticket. Say what it means; do not move it.

`dbt/README.md` has the run commands, and do not use `dbt build` on this tree.

## The write-up

Then `RESPONSE.md` at the root of the working tree, for the squad. Keep it short — it goes on
the ticket, not into a document. Five things, each with the figure behind it:

- how many orders `payment_match_is_ambiguous` is true of today, what the flag is really
  selecting, and how many orders in the estate are ambiguous the way the note describes —
  with the reason that number is what it is
- how many orders can come back carrying a different attempt from one build to the next
- of those, how many would publish a different `settled_net_cents` out of `int_payment_matched`,
  and the most that column could differ across the whole book between two builds
- the single worst day for it, and how many of that day's orders are on the list
- which of the payment columns the model publishes can move between builds, and which of them
  cannot move whatever the pick lands on

Whole numbers, in cents where it is money. Finance checks them.
