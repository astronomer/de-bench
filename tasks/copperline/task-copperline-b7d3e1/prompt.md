# RR-118: Meridian's dispute — restore what the landing copy still holds

Meridian Pay have disputed the settlement figures we billed against for February and March.
Legal have asked for our own reproduction of Meridian's event record for **settlement dates
2026-02-01 to 2026-03-31**, built from the landing copy of Meridian's files.

Not from `raw.pay_meridian_settlements`, and not from a mart. Those are our processing of the
same files, so a reproduction built from them agrees with itself and settles nothing. That is
the whole reason `fct_payments_restore` exists — `ops/incidents/2026-02-16-archive-gap.md` is
the note it was written after.

Nobody has ever run it. Someone tried on Monday and could not get it past its first step.

Two things, please.

## 1. Serve the request from the live landing tree

Restore into `ops.payments_restored` every Meridian event the live landing tree holds whose
settlement date falls in the requested range. One row per event, and these fourteen columns
and no others, as they land:

`event_id`, `payment_id`, `intent_id`, `order_ref`, `processor_txn_id`, `event_type`,
`amount_cents`, `currency_code`, `event_time_utc`, `loaded_at`, `settlement_date`,
`restates_event_id`, `attempt_no`, `deleted_at`

Nothing filtered, nothing netted, nothing filled in. A settlement date we cannot reach is a
settlement date with no rows, and that is the honest answer for it.

Run it through `fct_payments_restore`, and keep the DAG's four steps and their ids. The
finance on-call triggers it from the runbook and will not type dates in, so a plain trigger
with no run configuration has to serve RR-118. The run has to finish, too: a date the landing
copy cannot reach belongs in the note below, not in an exception that abandons the other
fifty-eight.

## 2. Write `RESPONSE.md` at the root of the working tree

Legal need to know what they are getting now and what is still to come. Three things:

- what the live landing tree served for the range, and why it reaches what it reaches
- which file dates an archive restore would have to cover before the rest of the range can be
  served, and what that costs us
- which dates we cannot serve at all, ever, and why

Name the dates. Keep it short — legal will paste it into the reply to Meridian.
