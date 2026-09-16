# data-platform conventions

Adds to the workspace `CONVENTIONS.md` and overrides it inside
`projects/platform/`. Style only. What this team's standards are — the
dependency rules, the warehouse rules, what a full refresh costs — is in
`README.md` beside this file.

## Naming

Every `dag_id` this team owns starts `plat_`. The prefix is how six teams share
one DAG list and how a search for platform work returns platform work. Other
teams spell their names differently and that is their business; inside this
folder the prefix is not optional.

A file is named after its `dag_id`. `plat_config_sync.py` holds
`plat_config_sync`, and a `*.dag.yaml` names the DAG it renders.

## Style

TaskFlow, with type hints. A task that takes a `list[dict]` says so, and a task
that returns nothing is usually a task that belongs inside another one.

Use a classic operator where the operator is the work — a `BashOperator` around
`dbt`, a sensor, an `EmptyOperator` as a named join. Do not wrap a shell command
in `@task` and call `subprocess`.

`@lake_task` from `include.lib.pipeline` is the house decorator and it is not
Airflow's `@task`. Read its docstring before you copy a neighbour; the three
differences it lists are the whole of what people get wrong.

## Fanning out

Map, never loop. A `for` loop over locations in a DAG file builds N tasks at
parse and the graph grows every time the list does. `expand()` builds one task
that runs N times, the UI stays readable, and a failed index retries alone.

Set `map_index_template` whenever the index is not self-explanatory, and cap the
fan-out with `max_active_tis_per_dag` where the mapped work writes.

## Scheduling

Cron. This team's work is the platform's clock, so it runs on one: the build at
04:00, the contracts at 06:00, the sweeps through the middle of the day. An
asset schedule is right where the trigger genuinely is another DAG's output, and
`plat_asset_republish` is the one that is.

## Paths and the warehouse

`include.lib.warehouse` for every path and every write. This team wrote the rule
that nothing outside `include/lib/` opens the warehouse, builds a path or writes
a partition, so this team does not get to be the exception.

## Documentation

A DAG carries `doc_md`, and the module docstring is what feeds it. Three
sentences: what it produces, where that lands, and who notices when it stops.
The person reading it at 02:00 is not the person who wrote it.
