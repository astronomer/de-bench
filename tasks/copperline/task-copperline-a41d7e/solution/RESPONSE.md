# PAY-207: September 2025 on the cards

September 2025 is **FY2025 P8**. It runs 2025-08-31 to 2025-10-04 on the 4-5-4 calendar in
`raw.fiscal_calendar`. It is not calendar September: 2025-08-31 opens it and 2025-09-30 is
four days short of its end.

The boundary is in `raw.pay_processor_windows`, which is the record and the only place the
switchover is written down as data. Halcyon Payments is authoritative to 2025-08-31,
Meridian Pay from **2025-09-01**. So the month has one Halcyon day, its first, and
thirty-four Meridian days.

Both feeds carry rows across most of it, on purpose. That is the shadow quarter
(`docs/runbooks/processor-migration.md` PRM-1), not a duplicate load, and the second feed
for a date is dropped rather than added.

## The month

| | Payments |
|---|---|
| Halcyon, 2025-08-31 | 2,542 |
| Meridian, 2025-09-01 to 2025-10-04 | 84,933 |
| **The month** | **87,475** |

Money, in integer cents, in the currency each payment was taken in. The three do not add.

| Currency | Halcyon day | Meridian | The month |
|---|---|---|---|
| USD | 74,702,371 | 3,025,835,566 | **3,100,537,937** |
| GBP | 21,659,348 | 750,290,982 | **771,950,330** |
| EUR | 24,708,895 | 927,516,405 | **952,225,300** |

Halcyon carries money as a two-decimal string, so it has to be parsed to cents rather than
summed. It never learned a currency column either, and on the boundary day
849 rows arrive with that column blank. The merchant account supplies it —
`merchant_acct` maps to a market through `raw.merchant_regions` and the market to its
currency through `raw.market_config` — and the 1,693 rows that do carry a currency agree
with the account on every one. Reading a blank as USD moves 16,433,649 cents into USD that
belong to sterling and the euro.

## What was left out

46,946 rows sit inside the month and are not counted, because the processor that sent them
was not authoritative on their date:

- 44,404 Halcyon rows dated 2025-09-01 to 2025-09-30. Halcyon kept sending for a month after
  it handed over, to prove nothing had been dropped.
- 2,542 Meridian rows dated 2025-08-31. Meridian had been live since 2025-07-01 and was
  still shadowing on the last Halcyon day.

Adding both feeds across the month instead gives 134,421 payments. That is the mistake the
runbook warns about, and it is half again as large as the month.

The two feeds agree exactly on 2025-08-31: 2,542 payments on each side, and the same cents
in each of the three currencies. That is the migration's own test passing, nine months late,
and it is why the boundary day can be checked rather than only asserted.

## Corrections

1,455 of the month's payments are restated by a later Meridian event. A correction belongs
to the date it restates, not to the date it arrives — `docs/reconciliation-policy.md` R-5,
and `docs/billing-integration.md` B-7, which says to take the highest revision of an event
and treat day 31 as a new event rather than a correction.

1,448 corrections land inside the month and belong to earlier ones. Counting those as
payments of this month, on top of the originals they do not belong to, gives 88,923 instead
of 87,475. No correction in this month moves an amount: every one of them restates the value
unchanged and moves only the date, which is worth saying because it means the money above
does not depend on which version is taken.

## Deleted at source

261 of the month's Meridian rows carry a `deleted_at`. They stay in, with their last known
amounts, flagged `deleted_at_source` — R-6 says a deleted row is neither filtered out nor
treated as missing. They are **inside** every figure above; they are worth 10,606,307 USD
cents, 2,102,451 GBP and 2,824,641 EUR. Dropping them takes the USD month to
3,089,931,630.

Halcyon has no delete stamp. Its table is frozen and cannot gain one.

## The orders behind them

**80,225 orders**, against 87,475 payments — 77,904 behind the Meridian days and 2,321
behind the Halcyon day, with no order in both.

The gap is split tender: 7,029 orders on the Meridian side paid with two card payments, and
each payment is its own attempt chain. 12,790 of the month's payments are a second or third
attempt on the same order.

`order_ref` on its own is not what counts them. It is seven digits over an order key that
runs past 180 million, so it recycles about every fifty days and one reference names three
orders across the estate. Joined to `raw.orders` on the reference alone it returns 1,269,154
orders for this month, which is more orders than the month has payments. The reference plus
a date settles it — the attempt's own date on the Meridian side, the transaction date on the
Halcyon side. Every payment in the month then finds exactly one order, at any window from
three days to thirty, and none is left over.

`raw.payment_intents.order_id` is not that key and will not join. It carries the processor's
own order number, not the `ORD-########` spelling `raw.orders` uses, and the join returns
nothing whatever it is cast to. Counted rather than joined it is still useful: the number of
distinct values behind the Meridian days is the same 77,904, which is the quickest way to
that figure and the reason the recycled reference is not needed for it.

## What each figure had to decide

**Which feed.** The windows table, per date, per `docs/billing-integration.md` B-8. Not the
date the second feed appeared: Meridian started sending 2025-07-01 and did not become
authoritative until two months later.

**Which clock.** The payment's own. Meridian's `event_time_utc`; Halcyon's `txn_datetime`,
which is merchant-local with no offset. The offsets in play are eight and nine hours ahead
of UTC and every capture on the boundary day falls between 03:10 and 13:00 local, so no row
changes date when it is converted. Dating on `loaded_at` instead gives 84,914 Meridian
payments; on `settlement_date`, 84,448; on Halcyon's `settled_on`, 2,923 rows for the
boundary day instead of 2,542.

**Which rows are payments.** Captures. Meridian's `authorized` events are the issuer holding
funds and are not money (B-1); there are 84,933 of them in the month, so counting them takes
it to 169,866. Halcyon's file is one row per transaction it took, so all three of its states
count — 2,534 settled, 7 reversed and 1 still pending on the boundary day.

Nothing in the pipelines was changed for this.
