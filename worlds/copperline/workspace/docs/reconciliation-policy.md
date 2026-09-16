# Reconciliation policy

Maintained by commerce with finance-eng. Last reviewed 2026-04-28. Reads: `raw.orders`, `raw.order_status_history`, the two processor settlement feeds, `raw.disputes`. Writes: `marts.recon_exceptions`.

Orders live in Copperline OMS. Money lives at the processor. They disagree every day, in both directions, and most of the disagreements resolve themselves within a day or two. This document is the precedence ladder that says who wins, in what order, so that the exception list holds the disagreements that need a person and nothing else.

The terms used below — event, capture, settlement, chargeback, revision — are defined in `docs/billing-integration.md` §B-1. That document defines the terms; this one governs which side wins. Neither overrides the other.

Work the clauses in order. R-1 through R-3 decide the state, R-4 decides whether the state is an exception yet, R-5 decides which period the correction lands in, and R-6 and R-7 cover what is left.

## R-1 Payment state: the processor wins

For anything about money — authorized, captured, settled, refunded, charged back, and the amounts attached to each — the processor is the system of record. Where the order record says one thing about payment state and the processor feed says another, take the processor. The order record is not corrected; the mart carries the processor's state and the disagreement is recorded.

## R-2 Fulfilment state: orders win

For anything about the goods — placed, picked, packed, shipped, delivered, returned — Copperline OMS is the system of record. A processor feed that implies a fulfilment state, and they do imply one, is describing its own view and not ours. Take the order record.

## R-3 Disputes override both

While a dispute is open, its state overrides both R-1 and R-2 for the payment it concerns. An open dispute is neither settled nor refunded, whatever either system says, and its amount is unknown until the case closes. `raw.disputes` with `closed_on` NULL is open. When the dispute closes, R-1 and R-2 resume and the outcome books under `docs/finance-policy.md` §REV-10.

## R-4 The two clocks

Every processor event carries `event_time` (the processor's clock) and `loaded_at` (ours). Age is measured on `event_time`, always. A disagreement whose processor event is less than 48 hours old is `pending_sync` and is not an exception, however long ago we loaded it. A disagreement whose event is 48 hours old or more is an exception, however recently we loaded it.

## R-5 Corrections and closed books

A processor correction inside the 30-day window (billing-integration §B-7) applies to the `ds` it restates. When that `ds` sits in a month closed under finance-policy §REV-8, do not restate it. Book one adjustment row dated the 1st of the earliest open month, with `exception_kind = 'closed_period_adjustment'`.

## R-6 Soft-deleted rows stay visible

A row deleted at source stays in the reconciliation output with `exception_kind = 'deleted_at_source'`. It is not filtered out, and it is not treated as a missing row. A source that deletes rather than cancels is telling us something, and dropping the row silently loses it. The row keeps its last known amounts.

## R-7 Unmatched rows

A processor event with no order, or an order with no processor event, is an exception in its own right once it clears R-4's 48 hours. Both directions are recorded, with `exception_kind = 'no_order'` and `exception_kind = 'no_payment'`. Neither is written off automatically and neither is netted against the other. The unmatched counts are the pair of numbers the payments squad reads first every morning.

## The exception kinds, in one place

| `exception_kind` | What it means |
|---|---|
| `amount_mismatch` | both sides have the payment, the amounts differ |
| `state_mismatch` | both sides have the payment, the states differ |
| `no_order` | the processor has a payment we have no order for |
| `no_payment` | we have an order the processor has no payment for |
| `deleted_at_source` | the source row was deleted, per R-6 |
| `closed_period_adjustment` | a correction that could not go back, per R-5 |
| `pending_sync` | a disagreement younger than 48 hours, per R-4 — not an exception |

`pending_sync` sits in the same table so that the daily counts are stable and a row can be watched as it ages across the line. It is a state, not an exception, and it is excluded from the exception totals anyone reports.

## The 48-hour line, and why it is on the processor's clock

The first version of this policy aged disagreements on `loaded_at`, because that is the column that was there. It meant that a backfill made two-week-old disagreements look one hour old, and the exception list emptied itself every time anyone replayed a window. Measuring on `event_time` makes age a property of the event rather than of our processing, which is what anyone reading the list assumes it is.

Both timestamps stay on the row. `loaded_at` is still the right column for questions about our own pipeline, and the wrong column for every question about the payment.

## A row worked through the ladder

A card payment on an order shipped yesterday. The order says captured for 8,400 cents. The processor says captured for 8,400 cents and refunded 8,400 cents an hour ago. There is no dispute.

- R-1: payment state is the processor's. The state is refunded.
- R-2: nothing here is about fulfilment, so R-2 does not apply.
- R-3: no open dispute, so nothing overrides.
- R-4: the processor event is an hour old on its own clock. Younger than 48 hours, so the disagreement is `pending_sync` and does not appear in today's exception total.
- R-5 to R-7: nothing to do.

Two days later the same row is 49 hours old and, if the order record still says captured, it is an `state_mismatch` exception. Nothing about the row changed. Its age did.

## Who reads the exception list

The payments squad works it every morning and clears most of it before the flash goes out. Finance reads the totals at close and cares about two kinds only: `amount_mismatch` inside a month that has not closed, and `closed_period_adjustment`, which tells them a correction arrived too late to go where it belonged.

Typical daily volumes, for a sense of scale rather than as a threshold:

| Kind | A normal day |
|---|---|
| `pending_sync` | 200 to 400 |
| `amount_mismatch` | 5 to 30 |
| `state_mismatch` | 10 to 60 |
| `no_order` | 0 to 5 |
| `no_payment` | 20 to 80, most of them cash and store credit orders |
| `deleted_at_source` | 0 to 3 |

`no_payment` is high because store orders paid in cash never reach a processor at all. That is expected and has been on the list since it was written.

## What the daily run does

`payments_recon_daily` compares orders to the processor feeds row by row and writes `marts.recon_exceptions`. It runs at 05:00 and takes about nine minutes. `fin_cash_recon_daily` is a different job with a different subject: it compares the bank file to what the processor said it sent, and it does not read this mart.

## Open items

- R-7 has never had a rule for how long an unmatched row stays on the list. Some are two years old. TODO: agree an aging rule with finance.
- The Halcyon-era rows have no `event_time`, so R-4 cannot be applied to them at all. They are all far older than 48 hours, so the outcome is the same and the reason is not.
- TODO: `deleted_at_source` fires about forty times a month and nobody has looked at what the source is deleting since the replatform.
- The dispute feed lands daily and the processor's own dispute view updates hourly. They drift inside a day. R-3 uses ours.
