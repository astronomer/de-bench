# CLS-208 — stand the nightly close up from cold

CLS-207 is settling the line arithmetic. This is the other half of the same
audit: the close's own steps, which is where the audit started. Four things came
out of it and they are all ours.

**The close cannot be run anywhere but production.** `load_price_book` reads
`marts.dim_product`. That relation is dbt's, `nightly_close` has no dependency on
the analytics build, and it is not in this warehouse at all. Standing a close up
on a copy of `raw` — which is what every investigation this quarter has needed —
stops on the first pricing step.

**One of the close's own checks cannot pass.** `check_pricing_ties` calls nearly
every order in the book a break. It is also wired so that it returns the count
rather than stopping on it, so in two years nobody has read the number.

**Two more of them cannot fail.** `check_economics_grain` and `tie_to_orders`
both read `marts.order_economics`, and `publish_economics` does not write it
until after both of them have run. On a date that has never been closed they
grade an empty table.

**The publish appends.** Close a date twice and the day is in the mart twice.
`CONVENTIONS.md`, "Writing to the warehouse", rule 2.

Get the close to finish from `load_price_book` through `publish_economics`
against the raw layer in this warehouse, and get the day it publishes right.
Work the steps in the order the graph runs them: each one that is wrong stops the
run, so you will meet them one at a time, and none of them is the only one.

## What the close has to publish

`marts.order_economics`, one row per order for the close date, to
`contracts/order_economics.yml`.

- **`booked_cents` is the order on the order date, gross.** The shared models
  under `dbt/` name the column that figure comes off in as many words. They are
  the reference here: read them, do not change them. `tie_to_orders` is what
  finance reads at close and it has to hold that figure against the order book,
  cent exact, on both sides.
- **Whose orders.** A staff transaction is not revenue, and the close already
  drops those in `enrich_base`, on the first step of the enrichment. A
  soft-deleted order stays: `docs/reconciliation-policy.md` R-6 says a row the
  source deleted still has to be accounted for, and dropping it here loses it
  everywhere below. **The shared spine under `dbt/` disagrees — it drops both —
  and on this the close does not follow it.** R-6 is the rule, `enrich_base` is
  what the rest of the close has to agree with, and the tie has to count the same
  orders on both sides.
- **The close does not publish a break.** A day that does not tie does not go
  into the mart. Whatever you move, keep that.

## The price book

The dimension `load_price_book` reads keeps one row per SKU and the price that
SKU is on today; the two columns the statement names are on no version of it. The
close prices the night it is closing, which may be months back, so today's price
is not the answer and a dimension that only knows today cannot give one.

Build the book from the feed the dimension is built from instead. The rule for
picking one version of a SKU out of that feed is written down under `dbt/`
twice — the price snapshot behind the dimension is one of them. Use it, and put
the close date on it. `staging.close_price_book` still has to come out of the
step, keyed `sku` and carrying `list_price_cents`: merch's realisation report is
why it exists, and the next statement reads it by those names.

## Out of scope, and we mean it

- **`close_line_economics`, `close_apply_promotions` and `returns_orphan_check`
  are CLS-207's and land this week. Do not touch them.** The line arithmetic and
  the returns join are settled there. What you need from them is that they run,
  and they do.
- `order_economics_daily` is a separate ticket and an older one. Its statements
  name columns the shared models do not have.
- The empty schemas and the mart's own DDL belong to the platform bootstrap, not
  to us. Stand them up by hand to run anything —
  `CREATE SCHEMA IF NOT EXISTS copperline.staging`, the same for
  `copperline.marts`, and `marts.order_economics` to its contract — and leave the
  bootstrap alone. They are already there on the close box.
- The export packs, the close log and the notification sit below the publish and
  work once there is a mart to read. Do not spend the afternoon down there.
- Nothing under `dbt/`, nothing in `contracts/`, nothing in another team's
  project.

## Running it

You cannot run this DAG end to end here. The four channel sensors want landing
files for the close date and the landing tree only carries the last few weeks.
Drive the pricing half by hand: every step from `load_price_book` down is a
callable that takes a date and nothing else.
