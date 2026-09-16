"""The DAG blueprint factory.

Most DAGs at Copperline are a file drop, a rollup, a dbt selection and an
export, wired in that order. The factory renders those from YAML so the shape
lives in one place and the DAG file holds only what is particular to it.
`docs/blueprints.md` is the guide; this package is the machinery.

    projects/growth/dags/comp_price_index.dag.yaml
    ------------------------------------------------------------------
    dag_id: gro_comp_price_index
    schedule: "0 7 * * *"
    description: competitor price index by market
    steps:
      land:
        blueprint: csv_intake
        source: comp_prices_{{ ds }}
        table: raw.comp_prices
      index:
        blueprint: rollup
        depends_on: [land]
        source: raw.comp_prices
        group_by: [market_id, lane_id]
        agg: median
        column: price_cents
        target: marts.price_index_daily

THE CONTRACT, in five sentences.

1. The top level takes `dag_id`, `schedule`, `description`, `default_args` and
   `steps`, and optionally `start_date`, `tags` and `notify`. `dag_id`,
   `schedule` and `steps` are required.
2. A step's name becomes its task id, so the step names are the graph.
3. A step takes a `blueprint`, an optional `depends_on` list of other step
   names, and whatever keys that blueprint needs. `depends_on` is the only
   wiring there is.
4. Unknown keys are IGNORED, not rejected. A misspelled key does not fail the
   parse; the step quietly does the default thing instead.
5. A rendered DAG is `catchup=False` and `max_active_runs=1`, because every
   kind here writes something the rest of the platform reads.

FIVE KINDS SHIP.

    csv_intake         lands a dated CSV from the landing tree into a table
    rollup             groups a source table and writes an aggregate
    dbt_select         runs one dbt selection
    partition_export   writes one partition of a table out as a file
    sensor_wait        waits for a file or a table before the steps after it

TASKS EXTEND THE FACTORY; THEY DO NOT REPAIR IT. This package is shared code
and it is protected: drive it, read it, never rewrite it. A ticket that needs
a step the five kinds do not cover adds a sixth kind from its own team, at
`projects/<team>/dags/kinds.py`, which the loader imports when it exists:

    from include.lib.blueprint import registry

    registry.register("sftp_drop", sftp_drop)

A builder takes `(step, ctx)` and returns ONE operator. It does not open the
warehouse, build a path or write a partition itself — that is `include/lib/`'s
job, and the builder calls into it.

A RENDERED DAG PAGES ITS OWNING TEAM, and the file does not have to say so.
YAML holds strings and numbers and never a callable, so `default_args` in a
blueprint cannot carry `on_failure_callback` the way a hand-written DAG's can.
The factory builds it instead, from the `notify` key or, when the file is
silent, from the team that owns the directory:

    notify: finance              # page finance instead of the owning team
    notify: false                # page nobody, deliberately
    notify:                      # the whole of it
      team: finance
      summary: ledger tie broke
      runbook: ops/runbooks/alerting.md

The callback goes on the DAG and into `default_args`, so every task inherits
it. DO NOT ATTACH A NOTIFIER AFTER `render_all`: assigning
`on_failure_callback` over a rendered DAG replaces the factory's rather than
adding to it, and a team that appends instead pages its rotation twice.

Two things worth knowing before you debug one:

- The factory renders at parse time, so a YAML error is an import error and
  shows up in the DAG list as a broken file.
- `sensor_wait` has no house timeout default. A step that omits `timeout` gets
  Airflow's own default of seven days, which is not a timeout. Three teams
  have been bitten and nobody has agreed what the default should be.
"""

from __future__ import annotations

from . import registry
from .registry import BlueprintError, MissingStepKey, register
from .render import (
    START_DATE,
    TOP_LEVEL_KEYS,
    Plan,
    RenderContext,
    Step,
    plan,
    render_all,
    render_file,
)

__all__ = [
    "registry",
    "register",
    "BlueprintError",
    "MissingStepKey",
    "Step",
    "Plan",
    "RenderContext",
    "plan",
    "render_file",
    "render_all",
    "START_DATE",
    "TOP_LEVEL_KEYS",
]
