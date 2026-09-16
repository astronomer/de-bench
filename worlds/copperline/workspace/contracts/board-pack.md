# Contract: Monday board pack

Schema: `contracts/account_rollup.yml`. Consumer C-1 in `docs/report-registry.md`. Owner: finance. Publisher: `fin_board_pack_weekly`. Agreed 2023-11-06, last amended 2025-06-02.

One CSV every Monday, read at the executive meeting the same afternoon. It is the oldest contract here and the one with the most eyes on its output.

## BP-1 Grain and columns

One row per fiscal month per reporting entity. Columns: `fiscal_month`, `entity`, `reported_cents`, `active_accounts`, and the top-20 account vector as 20 rows in the companion file. Cents are integers. `active_accounts` counts distinct accounts whose account status is active when the pack is built. It is a status count, not an activity count: an account with no orders in a year is active if its status is active, and an account that ordered yesterday is not active if its status is closed.

## BP-2 Exclusions

The pack excludes legacy-era plans (`billing_era = 'legacy'`). This is a presentation choice and does not change recognition; finance-policy governs recognition and this contract governs what the pack shows.

## BP-3 Grain is binding

The pack is monthly. A request to move it to another grain needs an amendment to this contract before any code changes, per `docs/change-management.md` §CM-1.

## The companion file

The top-20 vector ships as `exports/board_pack/{ds}_accounts.csv`, one row per account, ranked by `reported_cents` for the most recent complete fiscal month. Ties are broken by account id ascending, which has never happened and is written down anyway.

## The channel page

The week's channel split ships as `exports/board_pack/{ds}_channels.csv`, one row per day per channel for the week that closed on Sunday, columns `ds`, `channel`, `orders`, `booked_cents`. It reads the published `channel_daily` partitions rather than the mart table, because the pack is a record of what was published. A day with no partition file is left off the page; a partition with nothing under the header is a published zero and prints as one.

## Notes

- `reported_cents` is defined in `docs/semantic-definitions.md` and is not the same number as `recognized_cents`. The difference is refunds and the BP-2 exclusion, and it is a difference of whole rows rather than of pennies.
- The account count and the CRM's own count have never agreed. This one counts accounts with an active status; what the CRM export counts is not written down on this side, and asking has produced a different answer each time.
- The pack is built on Monday morning against whatever the marts hold at 09:00, so a mart that has not landed produces a pack that is short rather than late.

## Open items

- TODO: the ranking uses the most recent complete fiscal month, which in the first week of a month is two months back. Finance has never minded and it surprises everyone else.
- Nobody has written down what happens if an entity has no ledger coverage for a month in the pack's range.
