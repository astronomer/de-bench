# Report registry

Maintained by the data platform team. Last reviewed 2026-06-15, when PLAT-468 read three marts' rows back against the code. Regenerated with `docs/lineage.md` whenever a consumer is added; the two are written together on purpose.

Every published output Copperline produces, what publishes it, what it reads, where it lands, and who owns it. Business names are what people say in tickets — "the flash", "the board pack" — so this is the first place to look when a ticket names a report and nothing else.

## REG-1 Everything published is listed

Every published output appears in this table. A report that is not listed has no owner and no refresh commitment; do not build against one.

## REG-2 The read set is complete

The `Reads` column is the full read set, including readers dbt cannot see. `docs/lineage.md` holds the same facts the other way round, by table. If the two disagree, `docs/lineage.md` governs, and the disagreement is a bug — open a ticket.

## REG-3 Refresh times are commitments

Refresh times are commitments to the named owner, not to the platform team. The daily flash is due 06:00, the settlement summary 09:00 Monday, and the close file on the 6th business day, the day after the month closes.

## The registry

| # | Business name | Owner | Publisher | Reads | Lands at | Contract | Lineage |
|---|---|---|---|---|---|---|---|
| C-1 | Monday board pack | finance | `fin_board_pack_weekly` | `marts.revenue_recognized_monthly`, `marts.account_rollup`, `marts.dim_customer`, the published `channel_daily` partitions | `exports/board_pack/{ds}.csv` | `contracts/board-pack.md` | dbt ref; the channel page reads files |
| C-2 | Finance close file | finance | `fin_close_monthly` | `marts.revenue_recognized_daily`, `marts.revenue_recognized_monthly`, `raw.finance_ledger` | `exports/close/{fiscal_month}.csv` | `contracts/finance-close.md` | dbt ref |
| C-3 | Weekly settlement summary | commerce | `payment_settlement_weekly` | `marts.settlement_weekly` | email, plus `exports/settlement/{week}.csv` | `contracts/settlement-summary.md` | dbt ref |
| C-4 | Merch dashboards (7 files) | commerce | none — the BI tool reads the files | `marts.inventory_position`, `marts.sell_through_daily`, `marts.order_economics` | `dashboards/merch/*.sql` | `contracts/merch-dashboards.md` | literal SQL, off-lineage |
| C-5 | Daily flash | finance | `fin_daily_flash` | `marts.comp_sales_daily`, `marts.gmv_daily` | `exports/flash/{ds}.csv` | `contracts/daily-flash.md` | dbt ref |
| C-6 | Audience sync | growth | `gro_reverse_etl_ads`; the publish step is `scripts/publish_reports.sh` | `marts.audience_segments` | Beacon Ads, Tessera Social | `contracts/audience-sync.md` | cross-project, off-lineage |
| C-7 | Replenishment feed | supply | `sc_replenishment_daily` | `marts.inventory_position`, named in `config/replenishment.yml` | Ironwood ERP | `contracts/replenishment.md` | cross-project, off-lineage |
| C-8 | Customer 360 | customer | `cus_customer_360_daily`, published by `cus_360_publish` | `marts.customer_360`, `marts.dim_customer`, `marts.order_economics` | warehouse view and service-desk sync | `contracts/customer-360.md` | dbt ref; order history is literal SQL in `projects/customer/lib/profile.py` |
| C-9 | Compass feature table | customer | `cus_features_daily` | `marts.feature_customer_daily`, `marts.dim_customer`, `marts.order_economics` | `exports/features/{ds}.parquet` | `contracts/feature-store.md` | dbt ref; the behaviour windows are literal SQL in `projects/customer/lib/features.py` |
| C-10 | Data health alerts | growth | `gro_alerting_daily` | tables named as strings in `config/alerts.yml` | pager and email | `contracts/alerting.md` | config strings, off-lineage |
| C-11 | Kestrel vendor share | supply | `sc_partner_share_kestrel` | `marts.sell_through_daily`, picked up by a name-pattern sensor | SFTP drop | `contracts/partner-share.md` | pattern sensor, off-lineage |
| C-12 | Privacy queue | platform | `plat_privacy_sweep` | every surface `contracts/privacy.md` lists | deletion receipts | `contracts/privacy.md` | contract list only, off-lineage |

The `Lineage` column says how the consumer is discoverable, and it is the column worth reading before any change to a mart. Six consumers are ordinary dbt references and `dbt ls` finds them. A grep for the mart name adds C-4, whose dashboards hold the table name as literal SQL under a directory nobody builds. The other five hide behind a config string, a filename pattern, a contract list, and a publish script in another team's project. `docs/lineage.md` names all twelve.

## What each one is for

**C-1 Monday board pack.** One CSV every Monday morning, read at the executive meeting the same afternoon: company revenue by fiscal month, the top twenty trade accounts by reported revenue, the active account count, and the channel page for the week that closed on Sunday. Finance owns it. It has run every Monday since FY2023; the channel page is the one addition since.

**C-2 Finance close file.** Daily recognized revenue and the monthly rollup, produced once a month for the close. It has to tie to `raw.finance_ledger` to the cent for a closed month, which is the strictest commitment in this table. Finance reads it on the 6th business day; nobody else reads it at all.

**C-3 Weekly settlement summary.** The marketplace payout summary that goes to seller-ops as an email every Monday at 09:00, with the CSV beside it. Seller-ops read it to answer sellers who ask where their money is. Commerce owns it.

**C-4 Merch dashboards.** Seven `.sql` files under `dashboards/merch/`, each a literal query the BI tool runs on a schedule of its own. Merchandising reads them daily. There is no dbt reference, no Airflow task and no test anywhere in the path; the BI tool holds the schedule and the credentials. This is the least visible consumer in the estate and it is read by the most people.

**C-5 Daily flash.** Comp sales by region and marketplace GMV, out by 06:00 every morning. The most-read number in the company, and the one with the strictest time commitment. The comp figures follow `docs/comp-store-policy.md` and the prior-year comparison follows `docs/retail-calendar.md`.

**C-6 Audience sync.** Pushes `marts.audience_segments` to Beacon Ads and Tessera Social each morning for the day's campaigns. Growth owns it. The publish step is a shell script in the growth project, `scripts/publish_reports.sh`, rather than an operator, which is a leftover from before the reverse-ETL DAGs existed.

**C-7 Replenishment feed.** Pushes inventory positions to Ironwood every night so the ERP can raise purchase orders in the morning. Supply owns it, and the mart it reads is named in `config/replenishment.yml` rather than in the DAG. The final task compares its total against the source and fails when they differ, which is the only run-level guard on any consumer in this table.

**C-8 Customer 360.** One row per trade account with contact, order and payment attributes, rebuilt nightly and published as a warehouse view and a sync to the service desk. The customer team owns it and support reads it all day. It is the slowest model in the estate.

**C-9 Compass feature table.** Daily features for the Compass propensity model, written as parquet for the modelling team to pick up. Trade-account attributes come from a slowly-changing dimension, and the contract pins which version of an attribute a feature row carries.

**C-10 Data health alerts.** Freshness and volume alerting over the feeds, configured in `config/alerts.yml` by table name as a string. It pages growth's on-call. It is a consumer like any other and it is invisible to lineage, because a table name in a YAML file is not a reference.

**C-11 Kestrel vendor share.** A weekly SFTP drop of Kestrel Outdoor's own sell-through, so the vendor can plan its own production. Supply owns the relationship. The DAG picks up whatever matches the week's filename pattern, so a renamed output stops being delivered without failing anything.

**C-12 Privacy queue.** Works `ops/privacy/deletion_requests.csv` across every surface `contracts/privacy.md` lists, and writes a receipt per request. The platform team owns it. Its read set is a document rather than code, which makes it the one consumer that cannot be found by looking at the code at all.

## Refresh commitments

| Consumer | Due | To whom |
|---|---|---|
| C-5 daily flash | 06:00 daily | finance, and the executive team |
| C-7 replenishment feed | 06:00 daily | supply, and Ironwood |
| C-3 settlement summary | 09:00 Monday | seller-ops |
| C-1 board pack | 09:00 Monday | finance |
| C-2 close file | 6th business day | the controller's office |
| C-6 audience sync | 09:00 daily | growth |
| C-8 customer 360 | 07:00 daily | support |
| C-9 feature table | 09:00 daily | the modelling team |
| C-11 vendor share | Friday, close of business | Kestrel Outdoor |

C-4, C-10 and C-12 have no clock commitment. The dashboards refresh when the BI tool asks, the alerts run at 11:00 and page when they fire, and the privacy queue has a 30-day commitment from `docs/retention-policy.md` §RET-4 rather than a daily one.

## Readers that are not consumers

Two internal jobs read marts and publish nothing, so they are not in this table and are easy to forget. Both are in `docs/lineage.md`.

- `fin_ledger_tie` assembles its table name from `config/recon.yml` and reads `marts.order_economics` at order grain.
- `fin_gl_export` holds forty lines of SQL, no `ref()`, and reads the same mart at the same grain.

## How to use this list

A ticket usually arrives with a business name and nothing else: "the flash is wrong", "the board pack has two different customer counts". The first hop is this table, not a grep. It gives the publisher, and the publisher gives the models, and the models give the sources.

Going the other way — "I am about to change this mart, who breaks" — this table is the wrong shape. Use `docs/lineage.md`, which lists the same facts by table and includes the readers that publish nothing.

## The five that are invisible

Six consumers are ordinary dbt references. C-4 is findable by grep, because the table name is written in the dashboard SQL. These five are findable by neither, and each is invisible for a different reason.

| # | Why the graph cannot see it |
|---|---|
| C-6 | the publish step is a shell script in another team's project, called from the DAG |
| C-7 | the mart name is a string in `config/replenishment.yml`, assembled at run time |
| C-10 | every table it watches is a string in `config/alerts.yml` |
| C-11 | a `FileSensor` on a filename pattern, and no reference to the model that writes the file |
| C-12 | its read set is the surface list in `contracts/privacy.md`, which is prose |

None of these is a mistake to be fixed on the way past. Each is a real integration with a real owner, and each has been working for a year or more. They are listed here so that a change to a mart reaches them, which is what REG-2 is for.

## Contracts and schemas

Each consumer has a prose contract and a machine schema. The prose governs meaning, the schema is what `plat_contracts_enforce` runs every morning at 06:00, and `contracts/README.md` says what happens when they disagree.

| # | Contract | Schema |
|---|---|---|
| C-1 | `contracts/board-pack.md` | `contracts/account_rollup.yml` |
| C-2 | `contracts/finance-close.md` | `contracts/revenue_recognized_monthly.yml` |
| C-3 | `contracts/settlement-summary.md` | `contracts/settlement_weekly.yml` |
| C-4 | `contracts/merch-dashboards.md` | `contracts/order_economics.yml` |
| C-5 | `contracts/daily-flash.md` | `contracts/comp_sales_daily.yml` |
| C-6 | `contracts/audience-sync.md` | `contracts/audience_segments.yml` |
| C-7 | `contracts/replenishment.md` | `contracts/inventory_position.yml` |
| C-8 | `contracts/customer-360.md` | `contracts/customer_360.yml` |
| C-9 | `contracts/feature-store.md` | `contracts/feature_customer_daily.yml` |
| C-10 | `contracts/alerting.md` | `contracts/alert_subjects.yml` |
| C-11 | `contracts/partner-share.md` | `contracts/sell_through_daily.yml` |
| C-12 | `contracts/privacy.md` | `contracts/privacy_surfaces.yml` |

C-10 and C-12 are the two whose schema is not a mart. Their subject is a list — the tables watched, the surfaces swept — so the schema enumerates the list and the enforcement checks the list is complete.

## When something new is published

1. Add the row here, with an owner and a refresh commitment.
2. Add the reader to `docs/lineage.md`, under every table it reads.
3. Write `contracts/<consumer>.md` and the schema beside it.
4. Tell the owner what they are now committed to.

Skipping step 2 makes REG-2 false inside our own documents, which is the failure this list exists to prevent.

## History

| Date | What changed |
|---|---|
| 2023-05-02 | first written, seven consumers |
| 2024-08-14 | C-9 added when Compass went live |
| 2025-04-30 | `Lineage` column added after a mart change missed three readers |
| 2025-09-08 | C-11 added with the Kestrel agreement |
| 2026-01-26 | C-10 rewritten; the alerting config moved to table names as strings |
| 2026-05-18 | reviewed, no change |
| 2026-06-15 | PLAT-468: C-8 and C-9 gain `marts.order_economics` and `marts.dim_customer` |

## Open items

- The platform keeps its own rendered list of the finance reports, which is a third list and does not match this one. TODO: fold it in or say plainly that it is not this.
- The BI tool's own schedule for C-4 is not recorded anywhere Copperline controls.
- Nobody owns the seven dashboard files. Commerce is listed because commerce built them in FY2024.
