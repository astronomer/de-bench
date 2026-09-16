# finance-analytics conventions

Adds to the workspace `CONVENTIONS.md` and overrides it inside
`projects/finance/`. Style only. The standards — what ties to what, which
models may be incremental, when a month is closed — are in `README.md` beside
this file and in `docs/finance-policy.md`.

## Naming

Every `dag_id` starts `fin_`. Then the subject, then the cadence:
`fin_revenue_daily`, `fin_close_monthly`, `fin_board_pack_weekly`.

## Money

Integer cents. In a column, in a variable, in a return value, in a CSV. A float
in a money column is a defect and not a rounding style, and the contracts say so
in a rule the enforcement DAG runs. Where something has to be divided, divide in
cents and put the remainder somewhere on purpose — `docs/finance-policy.md`
§REV-2 says where.

## Style

Classic operators. This team reads its own DAGs in the first week of the month
with the controller's office looking over its shoulder, and a graph of named
operators is what that conversation wants. `PythonOperator` with a named
callable, a sensor where the wait is the work, `EmptyOperator` as a named join.

Work two DAGs share goes in `projects/finance/lib/`. Work that belongs to one
DAG stays in that DAG's module, above the graph, where the reader is already
looking.

No dynamic mapping in this project. A close pack has five entities and they are
five tasks, spelled out, so a failed entity is a red square with a name on it.

## Scheduling

Cron. The close DAGs run on the 1st and are not final until the 5th business
day — `include.lib.calendar.business_days_between` counts it, and it counts
half-open, so the 5th business day is the day where the count from the 1st is 4.

The daily work runs early, because the flash is due to its owner at 06:00 and
everything it reads has to have landed.

## Paths

`include.lib.warehouse` for every path and every write. Exports go under
`exports/<subject>/` and nowhere else, and the path is built by the library
rather than by an f-string in the DAG.

## SQL

SQL that a model could own belongs in a model. What is left in this project is
the tie, the export and the reconciliation — the things that read two systems
and compare them, which dbt has no good place for. Keep those readable and keep
them in one statement each.
