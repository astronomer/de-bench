# Contract: weekly settlement summary

Schema: `contracts/settlement_weekly.yml`. Consumer C-3 in `docs/report-registry.md`. Owner: commerce. Publisher: `payment_settlement_weekly`. Agreed 2022-09-19, last amended 2026-01-12.

The marketplace payout summary. It goes to seller-ops as an email every Monday at 09:00 with the CSV attached, and seller-ops answer sellers from it.

Columns defined by this contract and not in `docs/semantic-definitions.md`: `payout_cents`, `fee_cents`.

## SS-1 One row per seller per week

The grain is one row per seller per fiscal week. Not per settlement, not per order, not per payout batch. A seller with three payouts in a week has one row. A seller with no activity in a week has no row.

Columns: `week_start`, `seller_id`, `gmv_cents`, `commission_cents`, `fee_cents`, `payout_cents`. Cents are integers. `payout_cents` is what left Copperline's account, and it equals `gmv_cents` less `commission_cents` less `fee_cents` on every row.

## SS-2 No hand edits

The delivered summary is what the mart holds. Nothing is corrected by hand between the mart and the email, and no row is added, removed or adjusted in the spreadsheet before it goes out. Where a figure is wrong, the fix goes in the model and the week is republished with a note.

## SS-3 A week is delivered once

A week is summarised once, from the mart, for the seven days of that fiscal week. A re-run for a week that has already been delivered replaces that week's rows; it does not add to them. Two rows for the same seller and the same week is a defect, not a correction, and the totals will be wrong by exactly the amount of the second row.

## Notes

- `gmv_cents` is the seller's order value and is never Copperline revenue. `docs/finance-policy.md` §REV-14 owns that and sellers ask about it constantly.
- The marketplace settlement feed is the late one — about 71% of rows arrive on the day of their event, and the tail runs five days. A week summarised too early is short.
- Fee schedules change. When they do, seller-ops ask for a replay of the affected weeks, which is a normal request and is handled by re-running the weeks.

## Open items

- TODO: the email is assembled by the DAG's last task and the address list is in the DAG file. It should be in config.
- Sellers who left mid-week still get a row for the part-week. Nobody has asked for that to change.
- There is no receipt of what was sent. The CSV in `exports/settlement/` is the record, and it is overwritten by a replay.
