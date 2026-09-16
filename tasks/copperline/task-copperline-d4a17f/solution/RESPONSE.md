# PLAT-468 — three marts' rows, read back against the code

Fifteen rows in scope across the three marts. Four were not true. Three readers were missing.
Two registry rows understated their read set.

Out of scope and left exactly as they stand: the C-4 dashboard rows on `marts.order_economics`
and `marts.sell_through_daily`, the Compass export row and the C-12 privacy row on
`marts.feature_customer_daily`.

## The lineage rows

| Mart | Reader | Verdict | Evidence |
|---|---|---|---|
| `marts.order_economics` | `fin_ledger_tie` | keep | `config/recon.yml:7-9` gives schema `marts`, subject `order`, grain `economics`; `projects/finance/dags/fin_ledger_tie.py:37` assembles the three into the subject table |
| `marts.order_economics` | `fin_gl_export` | keep | `projects/finance/dags/fin_gl_export.py:67` — `FROM copperline.marts.order_economics oe` |
| `marts.order_economics` | `category_margin` | keep | `dbt/copperline_analytics/models/finance/category_margin.sql:53` — `from {{ ref('order_economics') }}` |
| `marts.order_economics` | `commerce_export_partner` | keep | `projects/commerce/dags/commerce_export_partner.dag.yaml:18` and `:28` — `source: marts.order_economics`. The path in the row said `.py`; the DAG is a blueprint and the row now says so |
| `marts.order_economics` | `fin_margin_daily` | remove | `projects/finance/dags/fin_margin_daily.py:33-34` builds `marts.category_margin` from `int.int_net_sales_lines`, and its docstring at `:8-15` names `int_net_sales_lines` and `int_order_lines_costed` as the two sources. Neither the DAG nor `projects/finance/lib/revenue.py` names this mart |
| `marts.order_economics` | `cus_customer_360_daily` (C-8) | add | `projects/customer/lib/profile.py:70` — `economics = warehouse.qualify("marts.order_economics")`, in `order_history`; the DAG imports the module at `projects/customer/dags/cus_customer_360_daily.py:36` |
| `marts.order_economics` | `cus_features_daily` (C-9) | add | `projects/customer/lib/features.py:78` — the same call, in `behaviour_windows`; the DAG imports it at `projects/customer/dags/cus_features_daily.py:35` |
| `marts.order_economics` | `gro_experiment_readout_daily` | add | `projects/growth/lib/experiments.py:91` — the outcomes build takes `net_sales_cents` off the mart; the DAG imports it at `projects/growth/dags/gro_experiment_readout_daily.py:28` |
| `marts.sell_through_daily` | C-11 Kestrel vendor share | keep | `projects/supply/dags/sc_partner_share_kestrel.py:57` — `FROM copperline.marts.sell_through_daily s` |
| `marts.sell_through_daily` | `gro_pricing_mart_daily` | keep | `projects/growth/lib/assets.py:70` defines `SELL_THROUGH`; the DAG imports it at `:30` and schedules on it at `:46`. The row said `ref`, which it is not, and now says asset schedule |
| `marts.sell_through_daily` | `sc_supplier_scorecard_weekly` | remove | `projects/supply/dags/sc_supplier_scorecard_weekly.py:49` and `:61` read `copperline.marts.int_po_receipt_matched`. The mart is named nowhere in the file |
| `marts.sell_through_daily` | `sc_shrink_weekly` | remove | `projects/supply/dags/sc_shrink_weekly.py:43-44` read `raw.wms_movements` and `raw.inventory_snapshots`; the DAG imports nothing from `projects/supply/lib/` |
| `marts.feature_customer_daily` | C-9 Compass feature table | keep | `projects/customer/dags/cus_features_daily.py:83` builds it, `projects/customer/lib/features.py:116` and `:138` read it back |
| `marts.feature_customer_daily` | `cus_churn_scores_weekly` | remove | `projects/customer/lib/churn.py:74` writes `ops.churn_features` and `:84` reads that table back. The mart it does read is `marts.customer_360`, at `:49`, where it is already listed |

## The registry

| Row | Changed | What it gained |
|---|---|---|
| C-8 Customer 360 | `Reads`, `Lineage` | `marts.order_economics`, per `projects/customer/lib/profile.py:70` |
| C-9 Compass feature table | `Reads`, `Lineage` | `marts.order_economics`, per `projects/customer/lib/features.py:78`, and `marts.dim_customer`, per `projects/customer/lib/features.py:45` and `:117` |

## What the audit says about the list

Every row that was wrong claimed an ordinary dbt reference on a job that has never held one, and
every reader that was missing sits in a lib module of another team's project. The rows the
document is proudest of — the config-assembled name, the literal SQL with no `ref` — were all
correct. What has rotted is the easy half.

Twelve marts have still not been read back.
