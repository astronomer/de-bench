# Vendor file loaded twice, 3 November 2025

Written 2025-11-04 by Mark Villeneuve, supply-chain.

## What happened

A carrier invoice file for 3 November arrived twice. The first copy landed at 02:14 and the second at 04:52, with the same name and the same contents. Both loads reported success. The day's invoice totals for that carrier came out at exactly twice the expected amount.

Nobody noticed on the day. The weekly freight accrual on the 7th was high by about a seventh, which is what got it looked at.

## Numbers

| | Rows | Amount |
|---|---:|---:|
| the file, as sent | 4,318 | $186,204.31 |
| loaded on the day | 8,636 | $372,408.62 |
| after the repair | 4,318 | $186,204.31 |

## What we did

Deleted the day's rows for that carrier and reloaded once from the archived copy. The accrual was rebuilt and the week came out right.

## Notes

- The carrier's own delivery log shows one send. The second copy came from a retry inside our own collection step after a timeout that turned out to be a slow write rather than a failed one.
- Both loads reported success because both loads succeeded. Nothing about the second load was an error.
- The same shape would go unnoticed on any table where a doubled day is not obviously wrong. Carrier invoices happened to be checked weekly against an accrual.

## Follow-ups

- Supply asked for a duplicate-file check on the landing path. Not done.
- TODO: work out how many other loads would silently double on a repeated file. Nobody has looked.
- The collection step's timeout was raised. That fixes this instance and not the shape.
