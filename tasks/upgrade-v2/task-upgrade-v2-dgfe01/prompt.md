This project pins `dag-factory==0.22.0` in `requirements.txt` and
uses it to generate DAGs from a YAML config. Upgrade `dag-factory`
to `1.0.1` and keep each generated DAG's observable behaviour
intact (same dag_id, same schedule, same task_ids). Airflow stays
on the version it's pinned to today.
