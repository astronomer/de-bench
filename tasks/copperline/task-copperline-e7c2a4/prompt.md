# PAY-249 — the card reconciliation has never matched anything

The payments squad works refunds and chargebacks out of the Meridian portal, by
hand, one at a time. `marts.recon_exceptions` was meant to have replaced that
two years ago and nobody on the squad has ever used it. Asked why, they say it
has never once given them a list they could work. Nobody there can remember
seeing an `amount_mismatch` or a `state_mismatch` come out of it, and
`docs/reconciliation-policy.md` says a normal day has tens of both.

J. Mwangi ran the first step of `payments_recon_daily` by hand last week and
stopped there. `staging.recon_matched` came back with a row for every order of
the day and a processor event on none of them: no `event_id`, no `event_type`,
no `processor_cents`, on any row, on any day she tried. So R-1 has nothing to
compare and R-7 has the entire order book to file as `no_payment`. That is the
whole of it: the reconciliation has never matched a card payment to an order.

Find out why the two sides do not pair up, and make them pair up.

## What we already know

- The feed is not empty and it is not the wrong feed.
  `raw.pay_meridian_settlements` carries the events, and the squad can find any
  of them by hand from the reference on the portal screen.
- It is not one bad day. She tried days months apart and got the same nothing.
- Whatever the answer is, it has to be the same rule for every day in the range,
  not a repair aimed at a day somebody quotes at you.

## What the step has to keep doing

- One row per order and processor event, for the day it is given. Every order of
  that day is in it once even when the processor has nothing for it — R-7 reads
  `no_payment` off exactly those rows, and it has to keep working.
- The step still reports the number of rows it wrote.
- Running the day twice leaves one copy of it.
- `raw.*` is landing and read-only. Whatever is wrong here, writing to a landing
  table is not the fix.
- The rest of the ladder is not in scope. Leave the finders, the ageing rule,
  the closed-period rule and the publish step alone, and do not add steps to the
  DAG. One clause per task is what makes a wrong number say which rule produced
  it, and this ticket is one statement.

Write down in the statement's own comment what you found and what the link is.
Whoever reads it next will ask the same question, and the answer should be
sitting there.

`docs/billing-integration.md` is the field reference for the processor feeds and
`docs/reconciliation-policy.md` is the precedence ladder the DAG works in order.
