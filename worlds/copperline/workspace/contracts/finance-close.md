# Contract: finance close file

Schema: `contracts/revenue_recognized_monthly.yml`. Consumer C-2 in `docs/report-registry.md`. Owner: finance. Publisher: `fin_close_monthly`. Agreed 2024-01-15, last amended 2025-08-25.

The close file is what the controller's office works the month-end close from. It is read once a month by four people and by nobody else.

Columns defined by this contract and not in `docs/semantic-definitions.md`: `ledger_amount_cents`.

## FC-1 The tie is cent-exact

For a closed fiscal month, the monthly total per entity equals `raw.finance_ledger` to the cent. Not within a tolerance, not within a rounding step: exactly. `ledger_amount_cents` ships beside `recognized_cents` on the monthly rows so that the difference is visible in the file rather than computed by the reader.

## FC-2 Grain and columns

Two grains in one delivery. The daily file is one row per business date per entity, with `recognized_cents`. The monthly file is one row per fiscal month per entity, with `recognized_cents`, `ledger_amount_cents` and the difference between them. Cents are integers throughout.

Daily rows cover the closed month only. The open month is not in the close file.

## FC-3 Closed months are final

A closed month is delivered once and does not change. Where a correction arrives for a month that has closed, it is not applied to that month's rows: `docs/finance-policy.md` §REV-8 says where it goes instead, and `docs/reconciliation-policy.md` §R-5 names the row it writes. A close file that changes after delivery is a defect, whatever the correction was.

## Timing

Delivered on the 6th business day, the day after the month closes. It is not delivered early on request, because a month that has not closed has no ledger to tie to.

## Notes

- Legacy-era plans are in this file. The board pack excludes them; this does not. The two are answering different questions and the difference between them is expected.
- The daily rows for legacy-era plans land on the first day of each fiscal month by design, per `docs/finance-policy.md` §REV-11. They look like spikes and are not.
- The ledger is maintained in Ironwood by the controller's office and lands as a monthly extract. It is not derived from anything in the warehouse, which is the whole point of the tie.

## Open items

- TODO: nothing in the file says which fiscal calendar version produced the month boundaries. It has not mattered.
- The difference column is signed and nobody has agreed which direction is positive. It is warehouse less ledger.
