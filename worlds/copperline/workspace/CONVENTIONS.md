# Conventions

The house rules, agreed by all six teams. Read this first. A team's own
`projects/<team>/CONVENTIONS.md` adds to it and overrides it inside that team's
directory, and nowhere else.

This file holds style. What a number means lives in `docs/`, what a consumer is
promised lives in `contracts/`, and who decides when two of those disagree lives
in `docs/change-management.md`.

## The tree

```
AGENTS.md            what this platform is, and where to start
CONVENTIONS.md       this file
include/lib/         the house library, imported by every project
config/              deployment config, the IaC surface, consumer config
plugins/             Airflow plugins
projects/<team>/     one directory per team: dags/, lib/, README.md
dbt/                 the dbt projects
contracts/           one contract per published mart
docs/                the policies, the registry, the runbooks
ops/                 incidents, runbooks, calendars, generated state
legacy/              the estate that was never ported. Text, not code.
landing/             where feeds arrive
fixtures/            the committed reference data and the finance files
exports/             what we send out
```

## Protected: drive it, read it, never rewrite it

- `include/lib/` — the house library. Its docstrings are the specification.
  Extend it through the documented entry points; do not fork a helper into a
  project.
- `include/lib/blueprint/` — the factory. A team that needs a sixth blueprint
  kind registers its own from `projects/<team>/dags/kinds.py`. See
  `docs/blueprints.md`.
- `contracts/` — amend a contract before the model, never after.
- `legacy/` — read-only. `include/lib/legacy_runner.py` shells it as a stub.
- `fixtures/` — read-only. `fixtures/reference/` is the market, entity,
  processor-window, gift-card-jurisdiction and carrier-rate-card data, committed
  because somebody types it rather than because a feed sends it, and mirrored
  into `raw`. `fixtures/finance/` holds the ERP ledger and the logistics cost
  workbook. Change a value here and you have changed the warehouse.

## Airflow

1. Import every authoring symbol from `airflow.sdk`, or from a provider path.
   Not `airflow.models`, not `airflow.decorators`, not `airflow.datasets`.
2. One file, one DAG, named after its `dag_id`. A factory and a `*.dag.yaml`
   are the two exceptions.
3. A `dag_id` is a contract with everything downstream. Do not rename one.
4. Write `schedule` out on every DAG, `None` included. Write `catchup` out too.
5. `start_date` is a fixed `pendulum.datetime(..., tz="UTC")` literal.
6. `max_active_runs=1` on anything that writes a shared table, which is nearly
   everything here: the warehouse is one DuckDB file and it takes one writer.
7. `tags` carry the team and the layer. `owner` is a team, never a person.
8. Two retries. More than three needs a comment saying what it is waiting out.
9. Every sensor sets a `timeout`. Airflow's own default is seven days, which is
   not a timeout.
10. Module level does no work: no warehouse open, no `Variable.get()`, no
    request, no heavy import. A committed YAML file read once at parse is
    allowed and is how the factories get their config.
11. `on_failure_callback=notify("<team>")` from `include/lib/notify.py`, on the
    DAG. Address it to the team that owns the DAG.

## Dates

**The clock is the run, not the wall.** `datetime.now()`, `date.today()` and a
database `current_date` are defects in this tree. A run is told which dates it
owns. Where a job genuinely needs to know what day it is now — a retention
cutoff, a report header — `include.lib.calendar.today()` is the one sanctioned
clock, and `docs/retail-calendar.md` is the authority on fiscal dates.

**A bare cron string is a trigger timetable.** That is Airflow 3's default and
we take it. So `{{ ds }}` is the day the run FIRES, and
`data_interval_start == data_interval_end`. Two consequences, and both are
house rules:

- A daily job that owns the day before names it, once, at the top of the file:

      TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

  and uses that everywhere a date reaches the data. `{{ ds }}` stays for log
  lines and file names that are about the run rather than about the day.
  `docs/late-data-policy.md` §LD-3 is why: a published daily output holds back
  one day, so the run on the 15th publishes the 14th.
- A window written `where ts >= '{{ data_interval_start }}' and ts <
  '{{ data_interval_end }}'` matches nothing at all under a trigger timetable.
  Take the window from `include.lib.watermark.since()` instead, which is
  interval-keyed and replays cleanly.

## Writing to the warehouse

1. Nothing outside `include/lib/` opens the warehouse, builds a path or writes a
   partition. `connect()`, `delete_insert()` and `write_partition()` are the
   whole surface.
2. Every write is scoped to a partition the run owns. A bare `INSERT` into a
   dated table is a defect: the second run of an interval puts the rows in
   twice.
3. Anything that only reads takes `connect(read_only=True)`. The live file takes
   one writer and a scheduled run holds it.
4. Qualify every table name. An unqualified name resolves against the frozen
   `nwv` copy first — `include/lib/warehouse.py` says why.
5. Money is an integer number of cents, in every table, at every step. A float
   in a money column is a defect, not a rounding style.

## dbt

1. One project, `dbt/copperline_analytics`. Four layers: `raw`, `stg`, `int`,
   `marts`. `projects/platform/README.md` holds the dependency rules and they
   are world policy, not platform preference.
2. Staging is one view per landed table, and it does not join.
3. A mart with a contract file does not change grain or drop a column until the
   contract is amended.
4. Cosmos renders the daily build, so one model is one task and a retry is per
   model. A `BashOperator` around `dbt` is for the case where one opaque
   invocation is genuinely what you want.

## Before you change a mart

Work `docs/lineage.md`. It lists every reader of every mart, including the ones
dbt and the Airflow graph cannot see: dashboard SQL, exports, readers assembled
from config, name-pattern sensors, and consumers in other teams' projects.
`dbt ls` is not that list.
