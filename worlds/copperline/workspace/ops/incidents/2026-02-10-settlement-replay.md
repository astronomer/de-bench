# Settlement replay, week of 2 February 2026

Written 2026-02-10 by Tomas Belka, commerce.

## What we did

On 9 February seller-ops asked us to re-run `payment_settlement_weekly` for 2 to 8 February after the corrected fee schedule landed. We cleared and re-triggered the seven runs that afternoon. Two of them were triggered twice because the first attempt was cleared while it was still queued. No errors. Settlement figures were not re-checked afterwards; seller-ops confirmed the fee schedule was right and closed the request.

## The dates

| Date | Runs |
|---|---:|
| 2026-02-02 | 2 |
| 2026-02-03 | 1 |
| 2026-02-04 | 1 |
| 2026-02-05 | 2 |
| 2026-02-06 | 1 |
| 2026-02-07 | 1 |
| 2026-02-08 | 1 |

## Notes

- The corrected fee schedule came from the marketplace team and covers three seller tiers that had been on the old rates since November.
- Clearing a queued run and re-triggering it is not something the interface stops you doing, and there is no warning.
- Seller-ops confirmed the fee schedule, which is what they had asked about. Nobody asked them to check the totals.

## Follow-ups

- TODO: check whether a replayed week is safe to re-run twice. Nobody has answered this.
- Commerce to use the backfill broker for replays rather than clearing runs by hand. Agreed, not adopted.
