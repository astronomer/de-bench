# PAY-233 — the refund recovery list has to name the right order

Refunds and chargebacks are worked out of a spreadsheet. Somebody pastes the
Meridian event export into it on a Monday, matches each event to an order by eye
on the reference, and reads the store and the market off whatever order came
back. It costs a morning a week, and the store totals in it have been wrong
often enough that seller ops stopped quoting them.

Build the table it should have been reading.

## What to build

`marts.settlement_attribution` — one row per Meridian settlement event, with the
order behind it.

| column | type | |
|---|---|---|
| `ds` | DATE | the event's own date |
| `event_id` | VARCHAR | |
| `intent_id` | VARCHAR | |
| `order_id` | VARCHAR | spelled as `raw.orders` spells it |
| `event_type` | VARCHAR | |
| `amount_cents` | BIGINT | |

The rules, and we will hold you to them:

- The day is the date of `event_time_utc`. Not the day the row loaded.
- Every row the feed carries for that day gets one row here, and every row here
  is one of them. Restatements and rows the processor later deleted are in —
  recovery works both. Do not filter and do not deduplicate.
- Every row names an order. `order_id` is never null, and a day's row count here
  is the feed's row count for that day. If you cannot make that true, do not
  paper over it.
- Rebuilding a day replaces that day and touches no other.

## Where it goes

- `projects/commerce/lib/attribution.py`, holding
  `build_daily(ds: str) -> int` — the number of rows written for the day.
- `projects/commerce/dags/psp_attribution_daily.py`, `dag_id`
  `psp_attribution_daily`, daily, with two steps: `build_attribution`, which
  calls `build_daily`, and `check_grain`.

It goes in `marts` because the squad reads it straight. Leave `contracts/` and
`docs/report-registry.md` alone — we will contract it once the recovery work
proves out.

## Two things the payments squad asked us to pass on

- `raw.payment_intents.order_id` is the payments ledger's own counter. It does
  not resolve against `raw.orders.order_id` and it never has. Nobody has had
  time to fix it.
- An attempt belongs to the order whose reference it carries and whose date it
  sits within a week of. `int_orders_enriched` spells that rule out.

`docs/billing-integration.md` is the field reference for the feed.
