# RTN-284 — the store credit the stores did not sell

Store finance closed last month three days late and the reason was returns.
A till hands a customer store credit for goods that came back over the
counter, the store books the credit, and the sale behind it was made
somewhere else. The stores want the credit taken off their books.
Merchandising says the web channel gave back nothing like that much. Neither
side can show a number, because there is no channel-level returns figure
anywhere in the platform: the close nets returns onto the order and never
splits them, and `marts.fct_returns` has two producers that do not agree on
what it holds.

Build the split.

## What to build

`marts.returns_by_channel_day` — one row per order day and channel.

| column | type | |
|---|---|---|
| `ds` | DATE | the day the order was placed |
| `channel` | VARCHAR | the channel that sold the line |
| `rmas` | BIGINT | return authorisations raised against that day's orders in that channel |
| `returned_qty` | DECIMAL(12,3) | units the authorisations cover |
| `refund_cents` | BIGINT | what the authorisations refund, whatever became of the money |
| `card_refund_cents` | BIGINT | the part of it that goes back to a card |
| `store_credit_cents` | BIGINT | the part of it that goes back as store credit |
| `counter_credit_cents` | BIGINT | the part of `store_credit_cents` the customer took at a till rather than by post |
| `returned_line_cents` | BIGINT | what those lines sold for: `line_total_cents` on the line each authorisation names |

The rules, and we will hold you to them:

- `ds` is the day the order was placed. Not the day the authorisation was
  raised, and not the day the goods came back. The row is read against what
  the channel sold that day, so it has to sit on the selling day.
- An authorisation counts once, in one row.
- A day and channel with nothing to report gets no row.
- A refund that went back another way stays out of `card_refund_cents` and out
  of `store_credit_cents`, and stays in `refund_cents`.
  `dbt/copperline_analytics/models/commerce/intermediate/int_return_linked.sql`
  says which book each one lands in and why the difference is not cosmetic.
- `counter_credit_cents` is the number the argument is actually about. A store
  that hands credit over its counter books the credit, and its own
  reconciliation shows it against a sale the store may never have made. So it
  is the till's share of the credit, and it is the till's share of the credit
  whichever channel the row belongs to.
- Which orders count is not a decision to make here. Every build in the tree
  already makes it, and makes it the same way. Make it the same way.
- A return against an order the OMS later deleted still counts. The order is
  gone upstream and the money is not; `docs/reconciliation-policy.md` R-6 is
  the rule and commerce applies it.
- A run owns one order day — the day it is given — and rebuilds that day whole,
  from every authorisation the feed carries against it, however long ago the
  authorisation was raised. A return comes in for two months after a sale, so a
  day is rebuilt many times before it stops moving; `docs/late-data-policy.md`
  LD-2 is the rule for what a rebuild does to what was there.
- A day is written whole and replaces what was there. Running a day twice
  leaves the same table.
- A run may not rebuild the table. `projects/platform/README.md` says what a
  full rebuild of a large table does to every other team's morning, and this
  runs every night.

## Where it goes

- `projects/commerce/lib/returns_channel.py`, holding
  `build_day(ds: str) -> int`, which builds one order day and returns the
  number of rows it wrote, and `tie_day(ds: str) -> int`, which fails the run
  when the day it wrote disagrees with the returns feed. We would rather lose
  a night than hand store finance a number the stores can pick apart.
- `projects/commerce/dags/returns_channel_daily.py`, `dag_id`
  `returns_channel_daily`, daily, with two steps, `build_day` and `tie_day`,
  each calling the function of the same name.

It goes in `marts` because store finance reads it straight. Leave `contracts/`
and `docs/report-registry.md` alone — we will contract it once the two teams
have agreed on one number.
