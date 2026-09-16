# customer

Owner: L. Baptiste. Sixteen DAGs covering identity, retention, loyalty,
consent and support — everything that answers "who is this account and what
has it done".

`CONVENTIONS.md` beside this file holds the style rules. This file says what
is here and why it is shaped the way it is.

## What this team owns

| Subject | What it means here |
|---|---|
| Identity | both account books, the crosswalk, the merge, `marts.dim_customer` |
| The 360 | `marts.customer_360`, the view support read, the service-desk sync |
| Features | the Compass feature table and its daily parquet |
| Support | tickets, and SLA attainment by queue |
| Loyalty and consent | points earned and burned, and who may be marketed to |
| Retention | churn scores and the weekly survey rollup |

Two of these are read hard outside this team. `marts.dim_customer` is a
conformed dimension the board pack and the feature table both join to, and
`marts.customer_360` is what support have open all day. Neither changes shape
without the readers in `docs/lineage.md` being worked first.

## Two books, and everything follows from that

Copperline bought Northwave Supply and the two account books were never
merged. That is the fact behind half the code here.

- `raw.customers` is Copperline's book, keyed `C-######`.
- `raw.nwv_accounts` is the acquired book, keyed `NWA-#####`, loaded once and
  never refreshed.
- `ops.merge_candidates` holds the three hundred pairs people reviewed by
  hand. It is the source of the merge decision and nothing re-derives one.
- Every customer-facing model carries `source_book`, and a join across the
  books goes through the crosswalk rather than a name.

`docs/runbooks/northwave-integration.md` is the state of the integration and
`docs/runbooks/customer-id-migration.md` is the state of the re-key. Read
both before touching the dimension.

## Hand-written and rendered

Thirteen DAGs are Python and three are YAML rendered by the blueprint
factory: `cus_loyalty_daily`, `cus_support_sla_daily` and `cus_nps_weekly`.
Those three are a file drop, a rollup, a dbt selection and an export in that
order, which is what the factory is for. The other thirteen have a shape of
their own — a paginated feed, a merge, an as-of join, a parameterised replay
— and `docs/blueprints.md` says plainly that a DAG like that is better
hand-written.

This project ships no `kinds.py`. Nothing here needs a step the five house
kinds do not cover, and a kind nobody uses is a kind somebody has to read.

## Schedules

Everything runs on cron. The chain in the morning is deliberate and tight:

| Time | DAG |
|---|---|
| 02:00 | `cus_customer_intake` |
| 02:30 | `cus_northwave_accounts_intake` |
| 03:00 | `cus_merge_candidates_review` |
| 03:30 | `cus_crosswalk_apply_daily` |
| 04:00 | `cus_dim_customer_daily` |
| 05:00 | `cus_address_normalize_daily` |
| 06:00 | `cus_customer_360_daily` |
| 07:00 | `cus_support_sla_daily`, `cus_loyalty_daily` |
| 08:00 | `cus_features_daily`, `cus_consent_sync_daily` |

`cus_360_publish` is the one exception and takes an asset:
`marts.customer_360` is the slowest model in the estate and its finishing
time moves by minutes from one morning to the next, so a clock would either
wait too long or publish half a table.

`cus_support_tickets_intake` runs hourly. `cus_churn_scores_weekly` and
`cus_nps_weekly` run on Friday. `cus_northwave_backfill` is triggered by
hand.

## The library

`lib/` holds the work; the DAG files hold the graph.

| Module | What is in it |
|---|---|
| `assets.py` | the one asset this team publishes and the one it waits on |
| `book.py` | folding Halyard's change feed onto the account book |
| `acquired.py` | reloading the acquired book, and the counts that never move |
| `merges.py` | the review sheets in, the decided pairs out |
| `identity.py` | the crosswalk, the merge, and the dimension over both books |
| `addresses.py` | normalising an address and keying it, columns in order |
| `profile.py` | the 360's legs, one subject each |
| `publish.py` | the published view, the service-desk sync, the hash boundary |
| `features.py` | the as-of join, the trailing windows, the parquet |
| `consent.py` | consent as a state, read as of the day |
| `churn.py` | the five inputs, the weights, and what makes a score wrong |
| `support.py` | the ticket feed and the party pair that joins it |
| `backfill.py` | replaying a mart into the era before the book landed |

## Three things worth knowing before you change anything here

**The merge is a decision, not a match.** Take it from
`ops.merge_candidates`. Re-deriving it by matching names, addresses or
domains gives a different set, because the population was chosen for being
hard: the same customer trades under two names, two different businesses
share a domain, and `info@` addresses and resellers are everywhere.

**The crosswalk applies by id format, not by order date.** A `CUST####`
reference goes through the map whatever date it carries. Switching on the
cutover date gives an answer that is complete and wrong by forty-five orders,
and sixty legacy ids have no map row at all and keep their own id.

**The 360 is slow and nobody has made it incremental.** About seven minutes a
day, eight joins over two years of events. It is due to support at 07:00 and
it starts at 06:00, so there is an hour of room and no more.

## Where things run

Everything here runs after the 04:00 dbt build, because the warehouse takes
one writer and a selection started while the build is running gets a lock
error rather than a queue. Anything that only reads takes
`warehouse.connect(read_only=True)`.
