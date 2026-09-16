# Mart lineage

Maintained by the data platform team. Last reviewed 2026-05-18. Written and regenerated together with `docs/report-registry.md`.

The registry lists consumers and says what each one reads. This document holds the same facts by table: for every mart, every reader of it, with the path of each. It is the list to work before a mart changes.

## LIN-1 This list is the checklist

Every consumer of every mart appears here, including consumers that dbt and the Airflow graph cannot see: dashboard SQL, exports, config-assembled readers, name-pattern sensors, and consumers in other teams' projects. Before you change a mart's grain, its columns or its partition key, work this list. `dbt ls` is not this list.

The `How` column says how the read is expressed. `ref` is an ordinary dbt reference and the graph knows about it. Everything else is a read the graph cannot see, and those are the rows this document exists for.

## `marts.revenue_recognized_daily`

| Reader | How | Path |
|---|---|---|
| `fin_close_monthly` | ref | `projects/finance/dags/fin_close_monthly.py` |
| `revenue_recognized_monthly` | ref | `models/finance/revenue_recognized_monthly.sql` |
| `fin_restatement_apply` | ref | `projects/finance/dags/fin_restatement_apply.py` |
| `fin_invoice_dispute_daily` | ref | `projects/finance/dags/fin_invoice_dispute_daily.py` |

## `marts.revenue_recognized_monthly`

| Reader | How | Path |
|---|---|---|
| C-2 finance close file | ref | `projects/finance/dags/fin_close_monthly.py` |
| C-1 board pack | ref | `projects/finance/dags/fin_board_pack_weekly.py` |
| `fin_ledger_archive_monthly` | ref | `projects/finance/dags/fin_ledger_archive_monthly.py` |
| `fin_budget_variance_weekly` | ref | `projects/finance/dags/fin_budget_variance_weekly.py` |
| `fin_report_publish` | config string | `config/reports.yml`, rendered by `projects/finance/dags/fin_report_publish.py` |

## `marts.account_rollup`

| Reader | How | Path |
|---|---|---|
| C-1 board pack | ref | `projects/finance/dags/fin_board_pack_weekly.py` |
| `revenue_by_book` | ref | `models/finance/revenue_by_book.sql` |
| `fin_ar_aging` | ref | `projects/finance/dags/fin_ar_aging.py` |
| `fin_report_publish` | config string | `config/reports.yml` |

## `marts.dim_customer`

| Reader | How | Path |
|---|---|---|
| C-1 board pack | ref | `projects/finance/dags/fin_board_pack_weekly.py` |
| C-8 customer 360 | ref | `projects/customer/dags/cus_customer_360_daily.py` |
| `feature_customer_daily` | ref | `models/customer/feature_customer_daily.sql` |
| `account_rollup` | ref | `models/finance/account_rollup.sql` |
| C-12 privacy queue | contract list | `contracts/privacy.md`, swept by `plat_privacy_sweep` |
| `cus_consent_sync_daily` | ref | `projects/customer/dags/cus_consent_sync_daily.py` |

## `marts.settlement_weekly`

| Reader | How | Path |
|---|---|---|
| C-3 settlement summary | ref | `projects/commerce/dags/payment_settlement_weekly.py` |
| the summary email | export | `exports/settlement/{week}.csv`, written by the same DAG |
| `fin_cash_recon_daily` | ref | `projects/finance/dags/fin_cash_recon_daily.py` |
| `dispute_daily` | ref | `models/commerce/dispute_daily.sql` |

## `marts.order_economics`

| Reader | How | Path |
|---|---|---|
| C-4 merch dashboards | literal SQL | `dashboards/merch/margin_by_category.sql`, `dashboards/merch/order_mix.sql` |
| `fin_margin_daily` | ref | `projects/finance/dags/fin_margin_daily.py` |
| `fin_ledger_tie` | config-assembled name | `projects/finance/dags/fin_ledger_tie.py`, from `config/recon.yml` |
| `fin_gl_export` | literal SQL, no ref | `projects/finance/dags/fin_gl_export.py` |
| `category_margin` | ref | `models/finance/category_margin.sql` |
| `commerce_export_partner` | ref | `projects/commerce/dags/commerce_export_partner.py` |

## `marts.inventory_position`

| Reader | How | Path |
|---|---|---|
| C-7 replenishment feed | config string | `config/replenishment.yml`, read by `projects/supply/dags/sc_replenishment_daily.py` |
| C-4 merch dashboards | literal SQL | `dashboards/merch/stock_cover.sql`, `dashboards/merch/out_of_stock.sql` |
| `sc_backorder_daily` | ref | `projects/supply/dags/sc_backorder_daily.py` |
| `sc_dc_capacity_daily` | ref | `projects/supply/dags/sc_dc_capacity_daily.py` |
| `fct_inventory_valuation` | ref | `models/supply/fct_inventory_valuation.sql` |

## `marts.sell_through_daily`

| Reader | How | Path |
|---|---|---|
| C-11 Kestrel vendor share | name-pattern sensor | `projects/supply/dags/sc_partner_share_kestrel.py`, matching `sell_through_*_{week}.csv` |
| C-4 merch dashboards | literal SQL | `dashboards/merch/sell_through.sql`, `dashboards/merch/markdown_candidates.sql` |
| `sc_supplier_scorecard_weekly` | ref | `projects/supply/dags/sc_supplier_scorecard_weekly.py` |
| `sc_shrink_weekly` | ref | `projects/supply/dags/sc_shrink_weekly.py` |
| `gro_pricing_mart_daily` | ref | `projects/growth/dags/gro_pricing_mart_daily.py` |

## `marts.comp_sales_daily`

| Reader | How | Path |
|---|---|---|
| C-5 daily flash | ref | `projects/finance/dags/fin_daily_flash.py` |
| `board_revenue_weekly` | ref | `models/finance/board_revenue_weekly.sql` |
| `fin_report_publish` | config string | `config/reports.yml` |
| C-10 data health alerts | config string | `config/alerts.yml` |

## `marts.channel_daily`

| Reader | How | Path |
|---|---|---|
| C-1 board pack, channel page | published partition files | `projects/finance/dags/fin_board_pack_weekly.py` |

The one mart here whose reader takes the published files, `include/data/marts/channel_daily_<ds>.csv`, rather than the table. The pack is a record of what was published, and the intake DAG's own docstring says which days publish at all.

## `marts.gmv_daily`

| Reader | How | Path |
|---|---|---|
| C-5 daily flash | ref | `projects/finance/dags/fin_daily_flash.py` |
| `channel_roi_daily` | ref | `models/growth/channel_roi_daily.sql` |
| `gro_experiment_readout_daily` | ref | `projects/growth/dags/gro_experiment_readout_daily.py` |
| C-10 data health alerts | config string | `config/alerts.yml` |

## `marts.audience_segments`

| Reader | How | Path |
|---|---|---|
| C-6 audience sync | cross-project script | `projects/growth/dags/gro_reverse_etl_ads.py`, publishing through `scripts/publish_reports.sh` |
| `gro_reverse_etl_crm` | ref | `projects/growth/dags/gro_reverse_etl_crm.py` |
| `gro_audience_export` | export | `exports/audiences/{ds}/` |
| C-12 privacy queue | contract list | `contracts/privacy.md` |

## `marts.customer_360`

| Reader | How | Path |
|---|---|---|
| C-8 customer 360 | ref | `projects/customer/dags/cus_360_publish.py` |
| the service-desk sync | export | `exports/service_desk/{ds}.json` |
| `cus_churn_scores_weekly` | ref | `projects/customer/dags/cus_churn_scores_weekly.py` |
| C-12 privacy queue | contract list | `contracts/privacy.md` |

## `marts.feature_customer_daily`

| Reader | How | Path |
|---|---|---|
| C-9 Compass feature table | ref | `projects/customer/dags/cus_features_daily.py` |
| the Compass export | export | `exports/features/{ds}.parquet` |
| `cus_churn_scores_weekly` | ref | `projects/customer/dags/cus_churn_scores_weekly.py` |
| C-12 privacy queue | contract list | `contracts/privacy.md` |

## `marts.daily_revenue`

| Reader | How | Path |
|---|---|---|
| `fin_report_publish` | config string | `config/reports.yml` |
| `fin_budget_variance_weekly` | ref | `projects/finance/dags/fin_budget_variance_weekly.py` |
| C-10 data health alerts | config string | `config/alerts.yml` |

## Why this document exists

A mart change that misses a reader does not fail. The reader keeps running against the old shape until the shape is gone, and then it either returns something wrong or waits forever for a file that is no longer written. Both have happened here: once when a grain change broke an order-level reconciliation in a dashboard nobody knew about, and once when a renamed extract left Kestrel Outdoor without a file for three weeks and nobody noticed until the vendor called.

Neither failure was found by a tool. Both would have been found by working a list.

## What this list does not include

Staging and intermediate models. This is marts and their readers. A change inside `stg_` or `int_` that does not change a mart's shape does not need this list; a change that does, does.

## Open items

- `plat_lineage_publish` regenerates `ops/lineage.json` from the dbt manifest every morning. That file holds the `ref` rows only, which is six of the twelve consumers. It is useful and it is not this list.
- TODO: the two internal readers on `marts.order_economics` — `fin_ledger_tie` and `fin_gl_export` — are the two most likely to be missed, because neither publishes anything anyone would think to ask about.
- Nobody has confirmed the seven dashboard files are still the seven. The BI tool can save a query without anyone committing it.
