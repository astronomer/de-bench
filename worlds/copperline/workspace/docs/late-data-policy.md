# Late data policy

Maintained by the data platform team. Last reviewed 2026-03-31.

Copperline takes data from three channels on three clocks and from a dozen third parties on theirs. Data arrives late every day. That is the normal state and not an incident.

## LD-1 Never drop late rows

Late arrivals are normal and must never be dropped. An incremental model's lookback window is the measured lag of its own source, and must be re-measured when that source changes. Measure it from `event_time` against `loaded_at`; do not copy a window from another model.

## LD-2 Restate the day, do not append to it

When late rows arrive for a day that has already been published, the day is rebuilt from its source and replaces what was there. Do not append the late rows to a published partition and do not write a second partition for the same day. An incremental model with a `unique_key` does this correctly; one without a `unique_key` appends, and the day quietly doubles.

## LD-3 Publication holdback

Published daily outputs hold back one day. A consumer reading `ds` on `ds` is reading an incomplete day, which is a bug in the consumer.

## How to measure a source

Every event feed carries two timestamps: `event_time`, which is when the thing happened on the source's clock, and `loaded_at`, which is when we got it. The lag of a source is the distribution of the difference, and it differs by source by more than most people expect.

Measure it on the source, over a stretch long enough to hold a peak day, and take the tail rather than the median. A window that covers the median covers nothing worth covering. A window copied from a neighbouring model covers whatever that model's source does, which is a different number.

## Two things people get wrong

**A row-level lag and a file-level lag are not the same number.** An in-store card settlement is never late as a row. The POS batch file that carries it can arrive up to three days after its business date, and when it does, every row in it arrives at once. A lookback tuned on row lag misses those files entirely; a lookback tuned on file arrival is far too wide for every other feed. Both numbers are right about different things and a model needs the one that matches how its source delivers.

**A wider window is not free.** Re-reading five days of a large feed every night costs five times the scan, and on the clickstream that is most of the run. Widen the window to the measured tail and no further.

## What the holdback means downstream

LD-3's one day is why the daily flash published on a morning covers the day before yesterday and not yesterday. People ask about this every few months. The alternative is publishing a number that moves after it is published, which finance likes less than the delay.

The holdback is a publication rule, not a processing rule. Models still process `ds` on `ds`; the published output is what holds back.

## Open items

- Nothing measures lag automatically. Somebody does it by hand when a model misbehaves. TODO: a daily lag profile per source into `ops.profile_daily` would cost about a day of work.
- The ad platforms restate spend three to seven days back and do not tell us. That is not lateness in the sense of this document, and nothing here covers it.
- The PIM change feed arrives out of order with no bound at all. The SCD contract handles it by ordering on the source's own change stamp rather than on arrival.
