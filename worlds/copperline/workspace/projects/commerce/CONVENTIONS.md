# Commerce conventions

These are commerce's house rules. They govern everything under
`projects/commerce/`. Where they and the workspace `CONVENTIONS.md` differ,
this file wins inside this directory and nowhere else.

Style only. What a DAG must do about correctness is in `docs/`, in
`contracts/` and in the library's docstrings.

## Names

A `dag_id` is **never prefixed**. Commerce owns the order spine, and the spine's
names are older than the team layout — `orders_intake` has been called that
since 2019 and half the company types it into a search box. Do not add `com_`,
and do not rename a DAG. A `dag_id` is a contract with everything downstream of
it.

The file is named for the DAG it holds: `orders_intake.py` holds `orders_intake`.
One file, one DAG. The blueprint loader, `blueprints.py`, is the one exception,
and it holds only the loader.

A task id says what the task does to what: `load_orders`, `build_economics`,
`export_partner_ironwood`. Not `task_1`, not `run_sql`.

## Cadence

Every intake is hourly and every rollup is daily. Nothing in between. If a feed
wants fifteen minutes, it is an intake and it runs hourly until somebody makes
the case; if a number wants an hour, it is a rollup and it runs daily.

Weekly DAGs exist for the two things that are genuinely weekly — the settlement
summary and the partner export — and both say so in the name or the docstring.

## Operators

Classic operators, with `retries` written out. An auditor reads the graph before
the run, and a graph made of named operators is the thing they read. TaskFlow is
fine in the projects that chose it; commerce did not.

**No dynamic mapping at all.** A mapped task is one square in the UI and N runs
underneath it, and the auditor's question — "which store failed" — then needs the
map index expanded. Write the branch out, or loop at parse time over a committed
list, or push the fan-out into SQL.

`@task.branch` is allowed and is the only decorator commerce uses, because a
branch is a graph decision and belongs in the graph.

## SQL

SQL lives in files, under `projects/commerce/sql/`, one statement per file. Read
one with `projects.commerce.lib.sql.read`. A statement in a Python string is a
statement nobody can run by hand when the number is wrong at seven in the
morning.

Bind values as parameters. Do not format a date into the text.

## Where things go

| Kind of thing | Where |
|---|---|
| the graph | `projects/commerce/dags/<dag_id>.py` |
| a step's logic | a module-level function in the same file, or `projects/commerce/lib/` |
| SQL | `projects/commerce/sql/<name>.sql` |
| a rendered DAG | `projects/commerce/dags/<name>.dag.yaml` |
| a commerce-only blueprint kind | `projects/commerce/dags/kinds.py` |

`include/lib/` is the house library and it is protected: drive it, read it, never
rewrite it. Nothing in this project opens the warehouse, builds a path or writes
a partition on its own.

## The DAG header

Every DAG carries, written out rather than defaulted:

- a module docstring, fed to `doc_md`, saying what it produces and who reads it;
- `schedule`, `start_date` as a fixed UTC literal, and `catchup`;
- `max_active_runs=1` wherever the DAG writes a shared table;
- `tags` naming the team and the layer;
- `default_args` with `owner`, `retries` and `retry_delay`, and nothing else;
- `on_failure_callback=notify("commerce")`.

Two retries is the default. Anything above three carries a comment saying what
it is waiting out.

## Reviewing one

Read the docstring, then the graph, then the SQL. If the three disagree, the
docstring is the one to fix first, because it is the one somebody read at seven
in the morning.
