# FIN-446 — the category margin daily has never published a day

`marts.category_margin` is empty. `fin_margin_daily` has been on the 06:30
schedule since we wrote it, `build` raises every night on a column the shared
models do not have, the two retries raise the same way, and the page has been
muted since last spring. Merchandising is arguing the FY2026 buy off a
spreadsheet because of it.

Make the build publish the day. `projects/finance/lib/revenue.py` holds it.

What the fix has to hold:

- One row per category for the day, and a category appears once.
- The day's `net_sales_cents` equals `int.int_net_sales_lines` for the same day,
  to the cent. That is `tie_to_sales`, it runs every night, and it is the check
  that catches a line counted twice.
- `landed_cost_cents` is the cost of goods on those same lines, to the cent —
  every line once, none of them dropped, and the number is the one the shared
  cost model already publishes at line grain. Finance does not compute cost of
  goods and does not derive it a second way; `projects/platform/README.md` rule
  4 is the reason that model exists. `docs/semantic-definitions.md` has an open
  item saying the words "landed cost" are older than the method, and the column
  on the mart keeps its name in this ticket.
- `merch_margin_cents` is `net_sales_cents` less `landed_cost_cents` on the row.
- The category comes off `marts.dim_product`, the catalog commerce builds. Read
  it. Do not rebuild it, and do not rebuild net sales.

Out of scope, and we mean it:

- `build_daily` and `marts.daily_revenue`. OPS-412 is open against that function
  and it is not yours. Leave it as you find it.
- Nothing under `dbt/`, nothing in `include/lib/`, nothing in another team's
  project. The shared models are the reference here — read them, do not change
  them. Commerce runs its own version of this join against the same two models
  and CLS-207 is theirs.
- You cannot run the DAG here. `int.*` and `marts.dim_product` are made by the
  04:00 dbt build and none of them is on disk, so read the models and work it
  out from the code.
