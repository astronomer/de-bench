# commerce

Owner: J. Mwangi. On call: the commerce rotation.
Mail: `commerce-data@copperline.example`.

Commerce owns the order spine — what sold, through which channel, at what price
and margin, and whether the money arrived. If a number anywhere in the company
starts with "how much did we sell", it starts here.

## What we publish

| Mart | Grain | Who reads it |
|---|---|---|
| `marts.order_economics` | one row per order | merch dashboards, `fin_margin_daily`, `fin_ledger_tie`, `fin_gl_export`, the partner exports |
| `marts.channel_daily` | market × channel × day | the board pack's channel page, from the published files |
| `marts.gmv_daily` | day | the flash, growth's channel return |
| `marts.settlement_weekly` | seller × fiscal week | seller-ops, `fin_cash_recon_daily` |
| `marts.recon_exceptions` | order × exception kind × day | the payments squad, finance at close |
| `marts.fct_returns`, `marts.refunds_daily` | RMA | recognition, disputes |
| `marts.cart_abandon_daily` | market × day | merchandising |
| `dim_product` | variant, SCD2 | everything that names a product |

`dim_product` is a conformed dimension and the platform team's policy says
platform builds those. This one never finished moving. It is built here, from
the snapshot pair `product_snapshot_daily` runs, under
`contracts/order_economics.yml`'s sibling contract that platform owns. Treat it
as shared: a change to it is a conversation, not a commit.

## The DAGs

Twenty-two. Seventeen hand-written, five rendered from `.dag.yaml` by the
blueprint factory. `docs/blueprints.md` says how the factory works and
`blueprints.py` is the loader that runs it for this project.

| Group | DAGs |
|---|---|
| order spine | `orders_intake`, `order_lines_daily`, `orders_enrich_daily`, `order_economics_daily` |
| the close | `nightly_close` |
| payments | `payments_intake`, `psp_bridge_build`, `payments_recon_daily`, `payment_settlement_weekly`, `fct_payments_restore` |
| marketplace | `marketplace_orders_na`, `marketplace_orders_eu`, `marketplace_settlement_intake` |
| stores | `store_pos_intake`, `store_pos_late_catchup` |
| product and price | `product_snapshot_daily`, `price_change_intake` |
| returns and refunds | `returns_daily`, `refunds_daily` |
| channel and web | `channel_daily_intake`, `cart_abandon_daily` |
| out | `commerce_export_partner` |

## Reading one

`CONVENTIONS.md` holds the style. The three things that catch people out are all
about dates and none of them is style:

- The order spine is keyed on `local_order_date`, the business date the store or
  the site kept. That is not `event_time_utc::DATE` for an evening sale in the
  Americas, and both columns are on the row.
- The marketplace recognises on `ship_confirmed_at`, one to six days after the
  order was placed, so a month boundary sits between the two on a few per cent
  of rows.
- The processor feeds age on the processor's own clock, never on when we loaded
  them. `docs/reconciliation-policy.md` R-4 says why.

## The documents that govern us

| Document | What it decides |
|---|---|
| `docs/reconciliation-policy.md` | which side wins when orders and the processor disagree |
| `docs/billing-integration.md` | what the processor feeds mean, field by field |
| `docs/finance-policy.md` | recognition, the refund boundary, closed books |
| `docs/late-data-policy.md` | how late rows land, and the publication holdback |
| `docs/runbooks/pos-ingestion.md` | the store batches, and what to do when one is missing |
| `contracts/order_economics.yml`, `contracts/settlement_weekly.yml` | the published grain and columns |
| `docs/lineage.md` | every reader of every mart, including the ones the graph cannot see |

`docs/lineage.md` is the one to work before a mart changes. `dbt ls` finds the
readers dbt knows about, and there are more readers than that.

## nightly_close

Thirty-four tasks in one file. It was three DAGs until the FY2025 close and
nobody has split it back. The ingest half takes about nine minutes; the finance
half takes forty seconds and is the one people rerun, which means clearing the
whole run. Its docstring carries the history.

## Layout

```
projects/commerce/
  README.md          this file
  CONVENTIONS.md     the team's style rules
  dags/              one file per DAG, plus blueprints.py and kinds.py
  dags/*.dag.yaml    the five rendered DAGs
  lib/               sql.py (the statement reader), pos.py (the batch tree)
  sql/               one statement per file
```
