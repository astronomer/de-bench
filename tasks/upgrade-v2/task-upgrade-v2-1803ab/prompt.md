This project generates its DAGs from YAML configs with `dag-factory`,
pinned to an old release in `requirements.txt`. Upgrade `dag-factory`
to the latest release. Preserve every DAG's observable behaviour and
task ids: same dag_id, same schedule, same task_ids, same task
dependencies, and the same pod configuration on every containerized
task. Airflow itself is moving to the version targeted for this
project.
