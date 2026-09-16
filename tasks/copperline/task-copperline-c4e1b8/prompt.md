# FIN-455 — the gift-card line of the close, off the ledger

The gift-card line of the monthly close is typed in from a spreadsheet and has
been since the acquisition. Nobody can say which cards are behind a month's
figure. Treasury cannot get an escheat number out of it at all, so they keep
their own workbook and the two of them have argued about February twice.

We hold the card book and we hold every entry against every card. Build the
table both sides read, and the spreadsheet goes.

## What to build

`marts.gift_card_recognition` — one row per recognition day, entity and card
currency.

| column | type | |
|---|---|---|
| `ds` | DATE | the day the amount recognizes |
| `entity_code` | VARCHAR | the entity the amount books to |
| `currency_code` | VARCHAR | the currency the card was sold in |
| `redeemed_cents` | BIGINT | redemptions recognized that day, in `currency_code` |
| `breakage_cents` | BIGINT | breakage recognized that day, in `currency_code` |
| `escheated_cents` | BIGINT | value that escheats instead of recognizing, in `currency_code` |
| `redeemed_base_cents` | BIGINT | `redeemed_cents` in USD cents |
| `breakage_base_cents` | BIGINT | `breakage_cents` in USD cents |
| `cards` | BIGINT | distinct cards behind the row |

The rules, and we will hold you to them:

- `docs/finance-policy.md` decides what recognizes, on which day, at which rate
  and to which entity. It is the authority and it is not up for improvement.
  Treasury's escheat column comes out of the same clause as the revenue, which
  is the whole reason both sides can read one table.
- Every amount in this table is positive. `raw.gift_card_ledger` is a liability
  ledger and its signs are the liability's, not ours.
- `escheated_cents` is the breakage the ledger wrote that the policy will not
  let us recognize, carried on the day it would otherwise have recognized on.
  When those balances actually go to a state is treasury's workbook and not
  this table's question.
- `raw.gift_cards.status` is the OMS's own word and the policy does not read
  it. The ledger is what happened to the card.
- Convert each entry once, on its own, and add the converted amounts. The
  rounding rule is `dbt/copperline_analytics/macros/money.sql`; the policy says
  at what rate.
- A row exists for every (`ds`, `entity_code`, `currency_code`) the ledger gives
  anything for on that day — a redemption, a recognized breakage, or an
  escheated one. A column with nothing in it is a zero, not a missing row.
- A run owns one recognition day and rebuilds it whole. Running the same day
  twice leaves the same table.
- A run may not rebuild the table's history. `projects/platform/README.md` says
  what a full rebuild of a large table does to every other team's morning, and
  this runs every night.

## Where it goes

- `projects/finance/lib/gift_cards.py`, holding `recognize(ds: str) -> int`,
  which rebuilds one recognition day and returns the rows it wrote, and
  `tie_cards(ds: str) -> int`, which fails the run when the day it wrote does
  not account for the ledger entries that day covers. We would rather lose a
  night than hand the close a third number.
- `projects/finance/dags/fin_gift_card_recognition_daily.py`, `dag_id`
  `fin_gift_card_recognition_daily`, daily, with two steps, `recognize` and
  `tie_cards`, each calling the function of the same name.

Leave `contracts/` and `docs/report-registry.md` alone. Finance will contract it
once the close has been run off it twice.
