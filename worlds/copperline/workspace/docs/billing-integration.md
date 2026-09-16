# Payment processor integration

Maintained by commerce with the payments squad. Last reviewed 2026-05-11. Source tables: `raw.pay_meridian_settlements`, `raw.pay_halcyon_settlements`, `raw.marketplace_settlements`, `raw.payment_intents`, `raw.pay_processor_windows`.

What the processor feeds mean. Copperline takes card money through Meridian Pay today and took it through Halcyon Payments until the migration finished. Both feeds are still in the warehouse and both are still read. This document is what a model needs to know before it joins either one to an order.

It says what the fields mean. It does not say who wins when the processor and the order management system disagree — `docs/reconciliation-policy.md` says that.

## B-1 What the terms mean

This document defines the vocabulary the reconciliation policy uses, and `docs/reconciliation-policy.md` governs which side wins when two systems disagree. Neither overrides the other.

- **event** — one row in a processor feed, keyed by `event_id`, describing something that happened to a payment.
- **authorization** — the card issuer holding funds. Not money.
- **capture** — Copperline claiming the held funds. Still not settled money.
- **settlement** — the processor moving money to Copperline's account. `settled_at` carries the date.
- **chargeback** — the cardholder disputing a captured payment. Opens a case; the amount is unknown until the case closes.
- **revision** — a restatement of an event the processor has already sent, carrying the original `event_id` and a higher `revision` number.

## B-2 The feeds and their keys

Meridian Pay delivers hourly JSON lines and carries `payment_id`, `order_ref`, `amount_cents`, `event_time`, `loaded_at`, `revision`, `settled_at` and `net_amount_cents`. Its durable key is `payment_intent_id`, reached through `raw.payment_intents`.

Halcyon Payments delivered a nightly CSV per merchant account and carried `txn_id`, `merchant_ref`, `amount_cents` and `settled_on`. It has no event time, no revision and no load stamp. It stopped sending on 2025-10-01 and the table is frozen.

Copperline Marketplace settlement arrives through the operator API and is a third shape again, keyed on `settlement_id` per seller per week.

## B-3 NULL semantics

`settled_at` is NULL until the processor settles. NULL never means "settled at load time". `net_amount_cents` is NULL on a chargeback until the case closes; NULL is unknown, not zero, and must not enter a sum. Rows with a NULL `event_time` are malformed and are rejected at load, not defaulted.

## B-4 Amounts and currency

`amount_cents` is the gross amount in the transaction currency, as an integer. `net_amount_cents` is gross less the processor's fee, in the same currency, and it is the amount that reaches the bank.

Neither feed carries a currency code. Both carry the merchant account, and the merchant account determines the currency: the mapping is in `raw.pay_merchant_accounts`. Reading either amount as USD is right for most rows and wrong for every row that settled through a non-USD account after the international launch — about 30% of Halcyon's volume from 2025-04-07 onward. Convert with `docs/finance-policy.md` §REV-7's rule, never with a rate looked up on the settlement date.

## B-5 Delivery and lateness

Meridian delivers hourly and about 92% of rows arrive on the day of their event; the rest arrive over the following five days. Halcyon delivered once a night with no tail. The marketplace feed is the late one: about 71% same-day.

An in-store card settlement is never late. The POS batch file that carries it can be up to three days late, which delays every row in the file at once, but no row inside it carries a lag of its own. Those are different facts and `docs/late-data-policy.md` §LD-1 says to measure each source rather than copy a window from another.

## B-6 Order references

`order_ref` is Copperline's reference and the processor recycles it across retry attempts inside a 24-hour window. It is not a key. The durable key is `payment_intent_id`, and orders reach payments through `stg_payment_intents`. A join on `order_ref` matches about 96% of rows and is wrong.

## B-7 The correction window

The processor restates events up to 30 days after the original event date. Day 30 is inside the window; day 31 is a new event. A restatement carries the original `event_id` and a higher `revision`. Always take the highest revision by `event_time`, then by `revision`.

## B-8 The overlap, and who is authoritative

Both processors sent settlements for the same payments through the migration quarter, on purpose. `raw.pay_processor_windows` holds the authoritative window per processor and is the record: Halcyon is authoritative through 2025-08-31, Meridian from 2025-09-01. Outside its authoritative window a processor's rows are shadow traffic and are not counted.

A union of both feeds over that quarter double-counts roughly a quarter of FY2025 settlement volume. Take the authoritative feed per date from the windows table; do not deduplicate by amount, and do not assume the switch happened when the second feed started.

## Meridian fields, in one place

| Field | Type | Notes |
|---|---|---|
| `event_id` | string | stable across revisions of the same event |
| `revision` | integer | 0 on the first send, higher on a restatement |
| `payment_id` | string | one per attempt, not one per payment |
| `payment_intent_id` | string | the durable key; one per payment |
| `order_ref` | string | Copperline's reference, recycled across retries |
| `amount_cents` | integer | gross, transaction currency |
| `net_amount_cents` | integer | gross less fee; NULL on an open chargeback |
| `event_time` | timestamp | the processor's clock, UTC |
| `loaded_at` | timestamp | ours, UTC |
| `settled_at` | date | NULL until settlement |
| `merchant_account` | string | determines the currency |
| `deleted_at` | timestamp | set when the processor removes a row |

Halcyon's four fields have no equivalent for most of these, which is why the two feeds are staged separately and unioned late rather than early.

## The marketplace feed

Copperline Marketplace settles sellers weekly, and the settlement file is written from the operator's side — Copperline is the operator, so this is our own money moving out, not a processor paying us in. The row is per seller per week and carries GMV, commission, fulfilment fees and the payout. `marts.settlement_weekly` is built off it and `contracts/settlement-summary.md` governs what the weekly summary shows.

The feed pages. The API returns at most 50 rows and a continuation token, and the token is absent on the last page only. The row count is not published, so nothing tells a caller how many pages there are.

## The 96% join

B-6 is the field note that costs the most time when it is missed. Meridian issues a fresh `payment_id` for every attempt on a payment, and reuses `order_ref` across the attempts, so a card retried twice produces three rows carrying the same `order_ref`. Join orders to payments on `order_ref` and most rows match, a few match twice, and the failures cluster on exactly the orders someone is asking about — the ones that were retried, which are the ones that went wrong.

`raw.payment_intents` is the bridge. One intent per payment, one or more events per intent, and `psp_bridge_build` maintains `stg_payment_intents` off it.

## What is not in this document

- Who wins a disagreement. `docs/reconciliation-policy.md`.
- What a settlement is worth in the ledger. `docs/finance-policy.md`.
- The marketplace commission rate card. That is in the seller agreements and the commerce team keeps a copy.
- Refund mechanics on the card rail. There is a page on the payments wiki and it has been out of date since Halcyon left.

## How the migration ran

The switch was planned as a shadow quarter and it ran as one. Meridian took live traffic from 2025-07-01 while Halcyon kept settling the same payments, so that two months of Meridian output could be compared against a processor everyone trusted. From 2025-09-01 the roles swapped: Meridian became authoritative and Halcyon kept sending for a month to prove nothing had been dropped on the way over. Halcyon's last file arrived 2025-09-30 and the feed stopped on 2025-10-01.

Nobody wrote the switchover date onto the rows. `raw.pay_processor_windows` has it, with two rows and three columns, and it is the only place it is recorded as data.

| Processor | `authoritative_from` | `authoritative_to` |
|---|---|---|
| Halcyon Payments | 2019-01-01 | 2025-08-31 |
| Meridian Pay | 2025-09-01 | NULL |

`docs/runbooks/processor-migration.md` covers what the migration did operationally and why the overlap is not a duplicate.

## History and open items

The Halcyon integration was written in 2019 and inherited its shape from the merchant statements, which is why it has a settlement date and no event time. Nothing about it will improve; the table is frozen at 2025-09-30 and read-only.

- TODO: `raw.pay_merchant_accounts` has 31 rows and four of them are for accounts that were closed before the fixture range starts. Harmless, and confusing every time someone counts accounts.
- The revision tie-break in B-7 has never actually tied. It is stated because it would be a coin flip if it did.
- Chargeback cases can stay open for months. Nothing ages them out, and `net_amount_cents` stays NULL the whole time.
