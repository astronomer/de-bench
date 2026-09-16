# PLAT-455 — C-10's watch list, reconciled

## Which side moves, and why

`contracts/alert_subjects.yml:7` says `source_of_truth: config/alerts.yml`, and
`contracts/alerting.md:22-23` §AL-3 says the same in prose: an alert exists because it is in
the config, which holds the table, the check, the threshold and the owner. So the config
defines what is watched, who is paged and what is checked. Where the schema disagreed about
one of those, the schema was the stale copy and the schema moved.

One rule runs the other way. §AL-1, `contracts/alerting.md:9`, says the owner has to be a team
that exists. That is a rule the config has to satisfy, not a fact it gets to state, and three
subjects broke it. Those three are the only config edits in this change.

## The config: three owners, and nothing else

`config/alerts.yml:78-96` shipped with a TODO saying three subjects named a team that no
longer exists. `lifecycle` and `acquisition` are not in `include/lib/notify.py:61`, so
`alerts.unowned` filtered them out of the paging step at
`projects/growth/dags/gro_alerting_daily.py:112-113`. All three have been measured and
recorded every day since January and have woken nobody.

| Table | Was | Now | Taken from |
|---|---|---|---|
| `raw.email_events` | `lifecycle` | `growth` | `projects/growth/dags/gro_email_engagement_daily.dag.yaml:17,24` |
| `marts.audience_segments` | `lifecycle` | `growth` | `dbt/copperline_analytics/models/growth/audience_segments.sql`, and C-6 is growth's at `docs/report-registry.md:28` |
| `raw.seo_rankings` | `acquisition` | `growth` | `projects/growth/dags/gro_seo_rank_intake.dag.yaml:19,28` |

Growth builds and runs all three feeds, so growth is the rotation a broken one should reach.
No threshold moved and no subject was added or removed.

## The schema: ten tables added, two rows corrected, five taken off

Ten tables the config watches were missing from the list, so the enforcement sentence in the
schema's own header could never have passed: `ops.session_daily`, `raw.ads_spend_daily`,
`raw.comp_prices`, `raw.pos_sales_header`, `raw.carrier_scans`, `marts.inventory_position`,
`marts.customer_360`, `raw.email_events`, `marts.audience_segments`, `raw.seo_rankings`.
Each is now named with the owner and the check `config/alerts.yml` gives it.

Two rows the schema already had were wrong about the table rather than missing it.

- `marts.gmv_daily`. The schema said `owner: finance` at
  `contracts/alert_subjects.yml:16`; the config says `commerce` at `config/alerts.yml:46`.
  Finance reads this mart for the 06:00 flash (`docs/report-registry.md:27`), but commerce
  builds it (`dbt/copperline_analytics/models/commerce/gmv_daily.sql`) and commerce is who a
  broken feed has to wake. The schema now says commerce. The config was not touched.
- `marts.comp_sales_daily`. The schema said `kind: freshness`; the config counts rows
  (`config/alerts.yml:48-51`). The schema now says `volume`, which is the word this file uses
  for a count (`config/alerts.yml:14`).

Five tables the schema named are watched by nothing at all: `raw.orders`,
`raw.pay_meridian_settlements`, `raw.inventory_snapshots`, `raw.carrier_invoices` and
`marts.daily_revenue`. They are the January list, kept after the rewrite replaced them. A list
that names them says C-10 covers five feeds it has never touched, so they came off it. Nothing
stopped being watched by that edit — none of the five was watched. The coverage question is
real and it is not this ticket: each needs a threshold argued from the feed, which is the shape
of GRO-241. Raised with commerce, supply and finance as PLAT-457.

Two subjects keep a word the schema has no vocabulary for. `raw.carrier_scans` and
`marts.customer_360` are `null_rate` checks and the schema only had `volume` and `freshness`,
so `kind: null_rate` is new. Nothing in the tree says what else it should be called.

`raw.web_events` is untouched in both files. GRO-241 has it.

## The third copy of the list

`docs/lineage.md` holds the same read set by table and REG-2 (`docs/report-registry.md:13`)
says it is the complete one. It listed C-10 as a reader of three marts and the config watches
five, so it disagreed with both other files.

- Added the C-10 row to `marts.inventory_position`, `marts.customer_360` and
  `marts.audience_segments`.
- Removed it from `marts.daily_revenue`, which C-10 has not read since January, and left a
  line saying why.

Only marts are affected. `docs/lineage.md:161` says staging and raw tables are out of that
document, so the eight `raw.` and `ops.` subjects have no row to add.

## What the enforcement sentence is worth today

Nothing. No code has ever compared the two files.

`plat_contracts_enforce` globs `contracts/*.yml` and runs each one, but a contract with no
`model` is not a mart contract: `include/lib/contracts.py:181-182` returns one violation saying
so, and `projects/platform/dags/plat_contracts_enforce.py:112-121` drops it before the failure
count, naming `alert_subjects` and `privacy_surfaces` as the two it skips. Its docstring says
the list check belongs to "the DAG that owns each document".

For C-10 that DAG is `gro_alerting_daily`, and it does not do it.
`projects/growth/dags/gro_alerting_daily.py:68-76` reports the subjects nobody owns — half the
sentence — and no step reads `contracts/alert_subjects.yml` at all. Neither does
`projects/growth/lib/alerts.py`, whose only inputs are the config and the quiet-day calendar.

So the schema has been a document since it was written, and the drift this ticket fixed is what
that costs. The check is a fifteen-line task beside `unowned` in `gro_alerting_daily`: load the
schema, compare the table sets, report a config subject the schema does not name. Raised as
PLAT-456. Nothing under `projects/` was changed for this ticket.
