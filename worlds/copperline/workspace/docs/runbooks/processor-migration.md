# Runbook: the payment processor migration

Owner: commerce, payments squad. Review date 2026-05-11. Advisory, per `docs/change-management.md` §CM-3.

Copperline moved card processing from Halcyon Payments to Meridian Pay across the third quarter of FY2025. Both processors sent settlement files for the same payments for a whole quarter. This runbook explains why, and what to do with the overlap.

`docs/billing-integration.md` defines the fields and `docs/reconciliation-policy.md` governs disagreements. This runbook is about the migration itself.

## PRM-1 The shadow quarter is not a duplicate

From 2025-07-01 to 2025-09-30 both feeds carry the same payments, on purpose. Meridian ran live from 1 July with Halcyon still settling behind it, so that two months of Meridian output could be compared against a processor everyone trusted; from 1 September the roles swapped and Halcyon ran a month of shadow traffic to prove nothing had been dropped on the way over.

Neither feed is wrong and neither is a duplicate load. A payment in that quarter genuinely appears twice, once per processor. Take the authoritative feed for the date and ignore the shadow rows. Do not deduplicate by amount, do not deduplicate by date, and do not assume the switch happened on the day the second feed appeared.

A union of both feeds across the quarter roughly doubles that quarter's settlement volume, which is about a quarter of the fiscal year. It is not subtle when it happens.

## PRM-2 Where the windows are recorded

`raw.pay_processor_windows` holds the authoritative window per processor and it is the record. Two rows, three columns: the processor, `authoritative_from`, `authoritative_to`. Read the window from that table rather than hard-coding a date, because the boundary is a business decision that was taken twice during the migration and moved once.

The switchover date was never written onto the settlement rows themselves. There is no column on either feed that says which processor was authoritative that day.

## The dates, for reference

| When | What was happening |
|---|---|
| before 2025-07-01 | Halcyon only |
| 2025-07-01 to 2025-08-31 | both feeds; Halcyon authoritative, Meridian shadow |
| 2025-09-01 to 2025-09-30 | both feeds; Meridian authoritative, Halcyon shadow |
| from 2025-10-01 | Meridian only; Halcyon's feed stopped |

`raw.pay_halcyon_settlements` is frozen and read-only. It will not grow again.

## Reading a date in the overlap

1. Look the date up in `raw.pay_processor_windows` and take the processor whose window contains it.
2. Read that processor's feed for the date.
3. Ignore the other feed for that date entirely. Its rows are shadow traffic; they are not corrections and they are not a second half of the day.
4. If the date is 2025-09-30 or earlier and both feeds are empty, that is a delivery problem, not a migration artefact.

The one place this is awkward is a payment authorized in August and settled in September. The authorization is in Halcyon's authoritative window and the settlement is in Meridian's, and both are correct. Take each event on its own date.

## Why the overlap was a whole quarter

The payments squad asked for two months of live comparison before trusting Meridian for money, and finance asked for a month of Halcyon behind it before letting the old integration go. Neither was willing to shorten their half, so the quarter is the sum of two requirements rather than one plan. It also happens to be a fiscal quarter, which was luck.

## What the migration left behind

- Two feed shapes that share no keys. Halcyon has a settlement date and no event time; Meridian has both timestamps, a revision number and an intent id.
- A recycled reference. Meridian reuses `order_ref` across retry attempts, so the join goes through `stg_payment_intents`. `docs/billing-integration.md` §B-6 has it.
- A correction window. Meridian restates up to 30 days back, which Halcyon never did.
- The billing-era split. Agreements signed before 2025-07-01 carry `billing_era = 'legacy'`, which is the same date and a different subject; `docs/finance-policy.md` §REV-11 owns it.

## Open items

- Nothing validates the windows table against the feeds. A wrong row there would be believed.
- TODO: the Halcyon staging models still run nightly against a frozen table. Harmless, and about four minutes a night.
- The reconciliation of the shadow quarter was done in a spreadsheet by the payments squad and signed off. The spreadsheet is on somebody's drive.
