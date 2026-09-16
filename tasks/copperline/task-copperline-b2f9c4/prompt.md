# PAY-251 — the cash sheet is short, and always in the same direction

Every morning somebody exports yesterday's Meridian events, pivots them by
currency in a spreadsheet and pastes the totals into the cash sheet. Three
times this year the sheet has come in under the processor's own monthly
statement — never over, in every currency, by well under a per cent. Finance
has stopped treating the daily number as anything but a shape.

Nobody re-exports a day once it has been pasted, so nobody has ever seen the
difference arrive.

Build the table the cash sheet should be reading.

## What to build

`marts.card_settlement_daily` — one row per settlement day per currency.

| column | type | |
|---|---|---|
| `ds` | DATE | the day the events happened |
| `currency_code` | VARCHAR | |
| `events` | BIGINT | |
| `captured_cents` | BIGINT | |
| `refunded_cents` | BIGINT | |
| `chargeback_cents` | BIGINT | |

The rules, and we will hold you to them:

- The day is the date of `event_time_utc`. Not the day the row reached us, and
  not `settlement_date`.
- `events` counts every row the feed carries for that day and currency,
  whatever its type. The three money columns sum `amount_cents` over the event
  type each is named after; an event of any other type counts in `events` and
  in none of the three.
- Every row counts once. Restatements and rows the processor later deleted are
  in — the netting is the reconciliation's job and not this table's, the same
  way `fct_payments` keeps both processors side by side rather than netting
  them.
- A day is written whole and replaces what was there. Running a date twice
  leaves the same table.
- A run may not rebuild the feed's history. `projects/platform/README.md` says
  what a full rebuild of a large table does to every other team's morning, and
  this runs every night.
- A run works from what had arrived by the end of the date it is given.
  `raw.pay_meridian_settlements` holds every hour we have ever taken and every
  row carries `loaded_at`, the hour it reached us. We replay nights when a
  month is disputed, and a replay that reads past its own night reports figures
  nobody could have published.

## Where it goes

- `projects/commerce/lib/card_settlement.py`, holding
  `load_delivery(ds: str) -> int`, which takes one date's delivery and returns
  the rows it wrote, and `check_days(ds: str) -> int`, which fails the run when
  the days it wrote do not tie back to the feed. We would rather lose a night
  than paste a short number.
- `projects/commerce/dags/card_settlement_daily.py`, `dag_id`
  `card_settlement_daily`, daily, with two steps, `load_delivery` and
  `check_days`, each calling the function of the same name.

It goes in `marts` because finance reads it straight. Leave `contracts/` and
`docs/report-registry.md` alone — we will contract it once the cash sheet has
run off it for a quarter.

`docs/billing-integration.md` is the field reference for the feed and
`docs/late-data-policy.md` is the policy over it.
