# The DAG blueprint factory

Maintained by the data platform team. Last reviewed 2026-04-14.

Most DAGs at Copperline are a file drop, a rollup, a dbt selection and an export, wired in that order. The blueprint factory renders those from YAML so that the shape lives in one place and the DAG file holds only what is particular to it.

`include/lib/blueprint/` holds the machinery. `registry.py` maps a blueprint name to a builder, `kinds/` holds the builders, and `render.py` walks a YAML file and wires `depends_on` into task dependencies. Every `projects/*/dags/*.dag.yaml` in the repo is rendered at parse time.

## The five kinds

| Kind | What it builds |
|---|---|
| `csv_intake` | lands a dated CSV from the landing tree into a warehouse table |
| `rollup` | groups a source table and writes an aggregate to a target |
| `dbt_select` | runs one dbt selection |
| `partition_export` | writes one partition of a table out as a file |
| `sensor_wait` | waits for a file or an asset before the steps that follow it |

## The YAML

```yaml
# projects/growth/dags/comp_price_index.dag.yaml
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
```

Top level takes `dag_id`, `schedule`, `description`, `default_args`, `notify` and `steps`. Each entry under `steps` takes a `blueprint`, an optional `depends_on` list of other step names, and whatever keys that blueprint needs. A step name becomes the task id.

`depends_on` is the only wiring. There is no way to express a dependency across two YAML files; a DAG that needs one takes an asset or a sensor.

## Who gets paged

A rendered DAG pages the team that owns its directory, and the file does not have to say so. This is the one place the factory does something the YAML does not spell out, and it is deliberate: YAML holds strings and numbers and never a callable, so `default_args` here cannot carry an `on_failure_callback` the way a hand-written DAG's can. If the file had to remember to name a team, the file that forgot would be the one whose failures went unnoticed.

Say `notify` when the default is wrong:

```yaml
notify: finance                      # page finance instead of the owning team
```

```yaml
notify: false                        # page nobody, deliberately
```

```yaml
notify:
  team: finance
  summary: ledger tie broke
  runbook: ops/runbooks/alerting.md
  page: false
```

The callback lands on the DAG and in `default_args`, so every rendered task inherits it. Do not attach a notifier after `render_all` — assigning `on_failure_callback` over a rendered DAG replaces the factory's rather than adding to it, and appending to it pages the rotation twice.

## Adding a kind

`include/lib/blueprint/` is shared code and the five kinds in it are the ones every team uses. A team that needs a sixth registers its own, from `projects/<team>/dags/kinds.py`, which the loader imports when it exists:

```python
# projects/supply/dags/kinds.py
from include.lib.blueprint import registry


def sftp_drop(step, ctx):
    """Write a rendered file to a partner's SFTP endpoint."""
    return SFTPPutOperator(
        task_id=step.name,
        local_path=ctx.render(step["local_path"]),
        remote_path=ctx.render(step["remote_path"]),
        ssh_conn_id=step["conn_id"],
    )


registry.register("sftp_drop", sftp_drop)
```

Once registered, the kind is usable from that team's YAML like any other:

```yaml
steps:
  drop:
    blueprint: sftp_drop
    depends_on: [export]
    local_path: "include/data/marts/sell_through_{{ ds }}.csv"
    remote_path: "/inbound/sell_through_{{ ds }}.csv"
    conn_id: kestrel_sftp
```

A builder takes the step and a render context and returns one operator. It does not open the warehouse, build a path or write a partition itself — that is `include/lib/`'s job, and the builder calls into it.

## When not to use a blueprint

A DAG whose shape is genuinely its own is better hand-written. `nightly_close` is the obvious one: thirty-four tasks, four channels and three exports, and expressing it as YAML would be a worse file than the Python. The factory is for the repetitive majority, not for everything.

## Notes

- The factory renders at parse time, so a YAML error is an import error and shows up in the DAG list as a broken file.
- `render.py` does not validate keys it does not know about. A misspelled key is ignored rather than rejected.
- Teams differ on how much they use it. Growth renders nearly everything; commerce hand-writes nearly everything. Both are fine.
- TODO: `sensor_wait` has no timeout default, so a DAG that omits one waits forever. Three teams have been bitten. Nobody has agreed what the default should be.
