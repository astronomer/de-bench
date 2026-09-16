# growth

Owner: S. Rasmussen. Eighteen DAGs covering traffic, marketing return,
pricing realisation and everything Copperline pushes back out to a system
outside the warehouse.

`CONVENTIONS.md` beside this file holds the style rules. This file says what
is here and why it is shaped the way it is.

## What this team owns

| Subject | What it means here |
|---|---|
| Clickstream | Driftwood's event stream, the sessions over it, and the funnel |
| Marketing | ad spend from three platforms, e-mail engagement, attribution, return |
| Pricing | the competitor feed, the price index, price realisation by market |
| Reverse ETL | segments back to the CRM, audiences out to the ad platforms |
| Alerting | the freshness and volume checks over the whole estate |

`marts.fct_web_sessions` is the denominator of conversion rate for the whole
company and `marts.audience_segments` is what two ad platforms target on
every morning. Both are read outside this team, and neither changes shape
without the readers in `docs/lineage.md` being worked first.

## Hand-written and rendered

Twelve DAGs are Python and six are YAML rendered by the blueprint factory.
The split is not a preference; it follows from what a blueprint can express.

**Rendered** — `gro_comp_prices_intake`, `gro_price_index_daily`,
`gro_market_seed_build`, `gro_audience_export`, `gro_email_engagement_daily`,
`gro_seo_rank_intake`. Each is a file drop or an API poll, a rollup or a dbt
selection, and an export, wired in that order.

**Hand-written**, and the reason each one is:

- Seven DAGs schedule on an asset, and a blueprint's `schedule` is a scalar
  in YAML. There is no way to write `schedule=[WEB_EVENTS]` in a blueprint
  file, so anything waiting on another DAG's output is Python.
- `gro_event_replay_repair` takes parameters, which a blueprint has no key
  for.
- `gro_clickstream_intake` reads a window from the watermark and holds a
  per-producer freshness rule.
- `gro_marketing_spend_intake` scopes its load to a platform AND a delivery
  date, and the house loader's replace scopes to one column.
- `gro_alerting_daily` maps over a config file.
- `gro_experiment_readout_daily` has two upstreams on two clocks.

A rendered DAG also carries no `outlets`, so nothing in
`projects/growth/dags/*.dag.yaml` publishes an asset and nothing waits on
one of them.

## The three team kinds

`dags/kinds.py` registers three builders the five house kinds do not cover.
`docs/blueprints.md` says how registration works.

| Kind | Why it exists |
|---|---|
| `api_intake` | `csv_intake` reads files; the fixture API pages |
| `seed_file` | a dbt seed is a committed CSV, and `dbt_select` will not seed |
| `export_file` | `partition_export` writes under `include/data/`; a reader outside the platform looks in `exports/` |

`include/lib/blueprint/` is shared code. A step the five house kinds do not
cover is a fourth kind here, not a change there.

## The library

`lib/` holds the work; the DAG files hold the graph.

| Module | What is in it |
|---|---|
| `assets.py` | every asset this team publishes or waits on, named once |
| `clickstream.py` | the landing tree, the load window, the per-producer lag bounds |
| `sessions.py` | sessions, the funnel, the touch-to-order join |
| `ads.py` | the delivery-grained spend feed and the restatements in it |
| `pricing.py` | the seed's market list against the config's, and the checks |
| `segments.py` | segment membership, consent, and the two destinations |
| `experiments.py` | exposures, outcomes, and whether the split held |
| `alerts.py` | reading `config/alerts.yml` and the agreed quiet days |
| `dbt.py` | one dbt selection, as an operator |

## Three things worth knowing before you change anything here

**Ad spend is one row per delivery, not one per day.** A platform delivers a
report date the next morning and delivers it again three to seven days later
with different numbers. Both rows stay. Anything that sums the table without
resolving a delivery per key counts about one key in eleven twice.

**The event stream holds replayed events twice.** One `event_id`, two stream
offsets, identical payloads — the January replay wrote recovered events into
fresh partitions. Count events by `event_id`.
`ops/incidents/2026-01-23-event-replay.md` has the whole story.

**The markets seed is a shorter list than `raw.market_config`.** The table is
the record of which markets exist; the seed is the list the models build
from. `docs/runbooks/market-setup.md` §MKT-1 owns the distinction, and
`gro_market_seed_build` is what moves a market from one list to the other.

## Where things run

Everything in this project runs after the 04:00 dbt build, because the
warehouse takes one writer and a selection started while the build is running
gets a lock error rather than a queue. Anything that only reads takes
`warehouse.connect(read_only=True)`.
