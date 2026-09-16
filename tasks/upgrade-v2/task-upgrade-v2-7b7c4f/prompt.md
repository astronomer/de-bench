This project uses `dag-factory` to generate its DAGs from YAML
configs, pinned to an old release in `requirements.txt`. Upgrade
`dag-factory` to the latest release. Every generated DAG must keep
its observable behaviour intact: same dag_id, same schedule, same
task_ids, same task dependencies. Airflow itself is moving to the
version targeted for this project.
