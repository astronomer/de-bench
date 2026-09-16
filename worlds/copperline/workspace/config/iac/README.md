# Deployment config

The Airflow variables, connections and pools this deployment runs with. Three
files, one per kind. `plat_config_sync` picks them up.

| File | Holds |
|---|---|
| `variables.yaml` | Airflow variables, by key |
| `connections.yaml` | connections, by `conn_id`. Secrets are references, never values |
| `pools.yaml` | pools and their slots |

## Editing

Edit here and the sync picks it up. Open a pull request like any other change;
the platform team reviews config the same way it reviews code.

## Secrets

A connection's password is a reference into the secret store, written
`secret://<path>`. A literal secret in these files is a defect and the review
will catch it. `plat_secret_rotation_check` flags a credential past its rotation
date on Monday mornings.

## Pools

A pool is how a team keeps one workload from eating the deployment. The
warehouse pool exists because the DuckDB file takes one writer: a job that
writes takes a slot in `warehouse_write`, and there is one slot.

## Notes

- The sync runs daily at 04:00.
- A new connection needs its secret in place before the sync runs, or the tasks
  that use it fail on the next open with a missing password.
- Names are what DAGs reference. Renaming a `conn_id` or a pool is the same kind
  of change as renaming a `dag_id`.
