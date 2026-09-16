# Clickstream feed decay, 11 to 14 January 2026

Written 2026-01-14 by Ines Okafor, growth. Reviewed with data platform 2026-01-16.

## What happened

The clickstream feed did not fail on 14 January. It faded. Daily event counts for the four days before the hard failure:

| Date | Events | Change on prior day |
|---|---:|---:|
| 2026-01-10 | 3,122 | +0.7% |
| 2026-01-11 | 2,638 | −15.5% |
| 2026-01-12 | 2,242 | −15.0% |
| 2026-01-13 | 1,888 | −15.8% |
| 2026-01-14 | 0 | hard failure |

Over the prior 90 days the feed ran 3,100 events a day with a standard deviation of 250, so a 2-sigma daily band sits at 16.1%. No single day before the failure fell outside that band, and by the 13th we had lost 39.5% of volume over three days and had already published three wrong daily flashes. A daily band was never going to catch this. What the shape has is persistence: three consecutive falls, none of them visible to a daily sigma test, and a level about 40% below normal.

## The cause

A collector release on the evening of the 10th changed how the Driftwood client batches events. Under the new batching a producer that failed to flush retried once and then dropped the batch. The failure rate climbed as the producer fleet rolled through the release, which is why the loss arrived in even daily steps rather than at once. On the 14th the last producer rolled and nothing flushed at all.

## What we did

- 14 January, 09:40: the daily flash came out at roughly two thirds of a normal Tuesday and finance asked why.
- 14 January, 10:15: growth found the feed at zero for the day.
- 14 January, 11:50: the collector release was identified and rolled back.
- 14 January, 14:00: the feed resumed at its normal level.
- 20 to 22 January: the missing window was replayed, which produced its own problem and its own note.

## What the alerting did

Nothing. `config/alerts.yml` holds a 2-sigma daily band on this feed. Each daily fall was inside the band and no alert fired on any of the four days, including the day the feed sent nothing — the zero-volume day is a freshness question and the volume check is what was configured.

Three daily flashes published on the 12th, 13th and 14th were built on incomplete clickstream data. They were not corrected, because by the time the feed was back the days had already been read.

## The wider picture

The 90-day baseline is stable and worth writing down, because it is the thing anyone reasoning about this feed needs and it is not recorded anywhere else.

| Measure | Over the 90 days to 2026-01-10 |
|---|---:|
| mean daily events | 3,100 |
| standard deviation | 250 |
| lowest ordinary day | 2,510 |
| highest ordinary day | 3,760 |
| days outside a 2-sigma band | 4 |

The four days outside the band were two known peaks and two regional closures. None of them was a defect, which is the other half of the problem: a tighter daily band would have paged on all four and been muted before January.

For contrast, the feed on the four decay days:

| Date | Events | Against the 3,100 baseline |
|---|---:|---:|
| 2026-01-11 | 2,638 | −14.9% |
| 2026-01-12 | 2,242 | −27.7% |
| 2026-01-13 | 1,888 | −39.1% |
| 2026-01-14 | 0 | −100% |

Each daily step is small. The distance from the baseline is not, and it grows on every one of the three days.

## What is worth taking from this

The daily band is the wrong shape for this failure and would have been the wrong shape for any gradual one. The numbers above are what a better rule has to be built from: the baseline level, the standard deviation over the prior 90 days, the size of each daily fall, and how far the feed had fallen cumulatively by the time it stopped.

Whatever replaces it also has to stay quiet on the days Copperline knows are quiet. `ops/calendar/quiet-days.yml` holds those, and the peak trading days are the same problem from the other side: a rolling standard deviation fires on the biggest sales day of the year unless the calendar exempts it.

## Follow-ups

- Growth to propose a replacement rule for `raw.web_events`. Not done at the time of writing.
- Collector releases to roll one producer at a time with a hold. Agreed with the collector team.
- Data platform asked whether any other feed is watched by a daily band alone. Not answered.
- TODO: nobody has decided whether a wrong flash gets corrected after the fact or left. This is the second time it has come up.
