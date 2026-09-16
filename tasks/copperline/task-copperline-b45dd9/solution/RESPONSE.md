# PAY-288: what the order reference is worth as a key

**It is not a key, and §B-6's 96% is not a measurement of anything.** Joined on the
reference alone, 227,041 of the feed's 2,058,581 events reach exactly one order — 11.0%.
The other 1,831,540 reach between 2 and 18, and not one event reaches none. That last part
is why nothing has ever looked broken: the join always returns something.

## How often the reference repeats

`order_ref` is `OE-` and seven digits, so the format holds **10,000,000** references. The
OMS hands out **200,000** order keys a day, and the reference is the key's last seven
digits, so the format is exhausted every 10,000,000 / 200,000 = **50 days**. Two orders
placed 50 days apart carry the same reference, and so do two placed 100 days apart, and so
on across the book.

That is measurable rather than assumed: over the 3,436,578 orders, 2,692,627 of the pairs
that share a reference and sit next to each other in date order are exactly 50 days apart,
and every other gap in the book is a multiple of 50 bar one pair.

§B-6 describes a different thing — the same reference reused across the retry attempts on
one payment, inside a day. That happens too, and it is not this. The 50-day recycle crosses
orders, and it is the one that misattributes money.

## The collision population in the order book

| | |
|---|---|
| Orders | 3,436,578 |
| Distinct references on them | 570,523 |
| Orders sharing a reference with at least one other | **3,107,760** (90.4%) |
| Orders holding a reference of their own | 328,818 |
| Most orders on any one reference | 18 |

The book runs 2024-02-04 to 2026-06-14, which is 862 days, or a little over seventeen
50-day cycles — so a reference in the middle of the book names 17 or 18 orders. 133,787 of
the 570,523 references name 17 or 18 between them.

## What a reference-alone join does to the feed

| | Rows |
|---|---|
| Events in `raw.pay_meridian_settlements` | 2,058,581 |
| Rows out, joined to `raw.orders` on `order_ref` alone | **29,968,433** |

14.56 rows out for every row in. The split by candidates:

| Orders the reference reaches | Events |
|---|---|
| exactly one | 227,041 |
| more than one | 1,831,540 |
| none | 0 |

**§B-6's 96% is wrong in the direction that matters.** 11.0% of the feed resolves to one
order, not 96%. Read the other way — how many events match *something* — the figure is
100%, not 96%. There is no reading of the data that produces 96, and the sentence has been
read for two years as if the failures were a rounding error.

## What it misattributes

| | Orders | Money (cents) |
|---|---|---|
| Reference-alone join | **3,096,290** | **1,530,264,361,359** |
| The orders that actually took a card payment | **918,830** | **105,596,982,367** |

So a reference-alone join hangs a Meridian event on 90.1% of the whole order book, and
2,177,460 of those orders never took a Meridian payment at all. The money it reports is
14.49 times the money the feed carries.

The honest side is the attempt behind the event: `raw.payment_intents` on `intent_id`, then
the order carrying the attempt's reference and placed within a week of the attempt's
`created_at`. That is one order per event across all 2,058,581 of them, with none left
over, and it holds for any window from 2 days to 45. At 49 days the recycle reaches the
neighbouring order and the row count goes to 3,579,951, so the window is exact rather than
lucky — it works because an attempt is raised a day or two after its order and the nearest
other claim on the reference is 50 days away.

## By period

Events are dated on `event_time_utc` against `raw.fiscal_calendar`.

| Period | Events | Rows out on the reference alone | Events resolving to one order |
|---|---|---|---|
| FY2025 P05 | 25,631 | 412,377 | 0 |
| FY2025 P06 | 140,469 | 2,276,619 | 0 |
| FY2025 P07 | 142,341 | 2,356,961 | 0 |
| FY2025 P08 | 180,209 | 2,937,644 | 0 |
| FY2025 P09 | 145,020 | 2,342,822 | 0 |
| FY2025 P10 | 345,273 | 2,596,987 | **198,260** |
| FY2025 P11 | 211,094 | 2,934,481 | **27,554** |
| FY2025 P12 | 149,007 | 2,429,958 | 1,227 |
| FY2026 P01 | 149,063 | 2,414,076 | 0 |
| FY2026 P02 | 187,892 | 3,049,188 | 0 |
| FY2026 P03 | 151,559 | 2,482,912 | 0 |
| FY2026 P04 | 148,896 | 2,368,958 | 0 |
| FY2026 P05 | 82,127 | 1,365,450 | 0 |

**Every unambiguous event in the whole feed comes off one trading day: 2025-11-28.** On an
ordinary day the OMS uses a few thousand of its 200,000 keys, so every reference it issues
is issued again 50 days later. On 2025-11-28 it placed 146,721 orders and ran deep into the
block, past the offsets any other day has ever reached — and 141,941 of that day's orders
carry a reference no other order in the book has ever carried. The settlements on those
orders land in P10, spill into P11 as the captures and refunds arrive, and trail into P12.

The same thing happened on 2025-12-01 (25,445 orders, 20,431 of them unique) and on the two
equivalent days a year earlier, 2024-11-29 and 2024-12-02, which are before the Meridian
feed starts and so contribute nothing to the table above.

The reading for audit is the wrong way round from the way anyone would want it: **the
reference is unique on the days we take the most money and ambiguous on every other day of
the year.**

## The key the documents send us to

`raw.payment_intents.order_id` does not name a Copperline order and never has.

| | Low | High |
|---|---|---|
| `raw.payment_intents.order_id` | 8,840,127 | 181,202,656 |
| The digits of `raw.orders.order_id` | 1 | 3,436,578 |

The two ranges do not overlap anywhere, so **no rows pair up — zero, on any cast.**
`order_id <= 3436578` is zero rows; stripping the `ORD-` prefix and joining on the digits
is zero rows; padding the processor's key into `ORD-########` is zero rows. The column
holds the processor's own order counter, not ours.

It is not useless, though, and it is the corroboration for the 918,830 above: the feed's
events sit behind 918,830 distinct values of that column, which is exactly the number of
Copperline orders the reference-and-date pairing reaches. The bridge is right about how
many orders there are and wrong about which ones they are.

## What we should tell audit

1. The reference attributes correctly on 11.0% of the feed by construction, and the control
   narrative claims it attributes on all of it.
2. Nothing in the estate fails when it goes wrong — the join always matches, so every
   downstream check has been green throughout.
3. `raw.payment_intents.order_id`, the key both §B-6 and `stg_payments__payment_intents`
   send a reader to, does not resolve. The workable link is the reference plus the attempt's
   own date, and it is exact over the whole feed.
4. §B-6 needs rewriting. It describes the within-payment retry recycle and misses the
   50-day cross-order one, and its 96% is not a figure this data can produce.
