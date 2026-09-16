# growth — how we write DAGs

Owner: S. Rasmussen. These rules add to the platform `CONVENTIONS.md` at the
repository root and override it inside `projects/growth/`. They are style. Read
them before you open a file here, then match the file you are in.

## Naming

Every `dag_id` starts `gro_`. After the prefix comes the thing produced and then
the cadence: `gro_price_index_daily`, `gro_seo_rank_intake`. A hand-written DAG
lives in `<dag_id>.py`; a rendered one lives in `<dag_id>.dag.yaml`. A `dag_id`
is a contract with everything downstream, so it is never renamed.

## Schedule on the asset that feeds you

A DAG that waits for another DAG's output takes that output as its schedule:

    from projects.growth.lib.assets import WEB_EVENTS

    @dag(schedule=[WEB_EVENTS], ...)

Not a clock, not a sensor, not an `ExternalTaskSensor`. The exception is a
source outside the platform that lands on a clock of its own — a vendor file
drop, an ad platform's morning delivery, an API we poll. Those take cron,
because there is no asset to wait for.

## Every asset is named once

`projects/growth/lib/assets.py` holds every `Asset` this team publishes or
subscribes to, one name each. Refer to it by that name. A URI written out in a
DAG file is a URI that will disagree with somebody else's spelling of it, and
the run that never fires is the one nobody notices.

## TaskFlow, typed, and every task returns something

`@task` and `@lake_task` from the SDK, with type hints on the arguments and the
return. A task that returns nothing has either done no work worth recording or
is doing its work somewhere the manifest cannot see it; both are worth a second
look before it ships.

`@lake_task` is the house decorator and it is not Airflow's `@task`. Read its
docstring in `include/lib/pipeline.py` before you copy a neighbouring file.

## Config belongs in YAML; the Python is a renderer

Most of what this team runs is a file drop, a rollup, a dbt selection and an
export. That shape belongs in `<dag_id>.dag.yaml` and the blueprint factory
renders it — see `docs/blueprints.md`. Reach for Python when the DAG's shape is
genuinely its own: an asset schedule, a `Param`, a branch, a loop over
configuration.

A step the five house kinds do not cover is a kind of this team's own, in
`projects/growth/dags/kinds.py`, registered through `registry.register`. It is
never a change to `include/lib/blueprint/`, which is shared code.

## Map freely

Dynamic task mapping is the way this team fans out. `expand()` over a list a
task returned, `partial()` for the fixed arguments, and `map_index_template`
whenever the index is not readable on its own.

## Paths and the warehouse

Warehouse work goes through `include.lib.warehouse`. Partition files go through
`write_partition`. Landing paths are templated strings in the DAG or the YAML,
never assembled from the clock.
