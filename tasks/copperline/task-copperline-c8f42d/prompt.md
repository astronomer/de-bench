# RTN-266 — three returns numbers, and one of them goes negative

Merchandising asks every month what a day's returns cost us and gets a
different answer depending on who they ask. The close nets returns into
`marts.order_economics`, the dbt fact nets them again at line grain, and the
returns job counts authorisations on the day they were raised. On the close's
own mart `net_sales_cents` has come back negative on days we sold plenty, and
the run that wrote it went green. Somebody else has the close; nobody is going
to reconcile all three this quarter.

Build the one table we will argue from.

## What to build

`marts.returns_by_order_day` — one row per day we sold.

| column | type | |
|---|---|---|
| `ds` | DATE | the day the order was placed |
| `rmas` | BIGINT | return authorisations raised against that day's orders |
| `open_rmas` | BIGINT | of those, the ones we have no receipt for |
| `returned_qty` | DECIMAL(12,3) | units the authorisations cover |
| `refund_cents` | BIGINT | what the authorisations refund, whatever became of the money |
| `card_refund_cents` | BIGINT | the part of it that goes back to a card |
| `returned_line_cents` | BIGINT | what those lines sold for: `line_total_cents` on the line each authorisation names |

The rules, and we will hold you to them:

- `ds` is the day the order was placed. Not the day the authorisation was
  raised, and not the day the goods came back. Returns belong to the day that
  sold them or the number tells merchandising nothing.
- An authorisation counts once, on one day.
- A refund that went back another way stays out of `card_refund_cents` and
  stays in `refund_cents`.
  `dbt/copperline_analytics/models/commerce/intermediate/int_return_linked.sql`
  says which book each one lands in and why the difference is not cosmetic.
- Which orders count is not a decision to make here. Every build in the tree
  already makes it, and makes it the same way. Make it the same way.
- A return against an order the OMS later deleted still counts. The order is
  gone upstream and the money is not; `docs/reconciliation-policy.md` R-6 is
  the rule and commerce applies it.
- A run owns one night of the returns feed — the authorisations raised on the
  date it is given — and rebuilds, whole, every order day that night's
  authorisations name. Returns reach a long way back. A window carried over
  from another job is what `docs/late-data-policy.md` LD-1 is about.
- A day is written whole and replaces what was there. Running a night twice
  leaves the same table.
- A run may not rebuild the feed's history. `projects/platform/README.md` says
  what a full rebuild of a large table does to every other team's morning, and
  this runs every night.

## Where it goes

- `projects/commerce/lib/returns_netting.py`, holding
  `net_delivery(ds: str) -> int`, which takes one night's authorisations and
  returns the number of order days it rewrote, and `tie_days(ds: str) -> int`,
  which fails the run when a day it wrote disagrees with the returns feed. We
  would rather lose a night than hand merchandising a fourth number.
- `projects/commerce/dags/returns_netting_daily.py`, `dag_id`
  `returns_netting_daily`, daily, with two steps, `net_delivery` and
  `tie_days`, each calling the function of the same name.

It goes in `marts` because merchandising reads it straight. Leave `contracts/`
and `docs/report-registry.md` alone — we will contract it once the number has
stood up for a quarter.
