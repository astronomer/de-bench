This project pins `dag-factory==0.22.0` in `requirements.txt` and
generates DAGs from a small fleet of YAML configs. Land it on
`dag-factory==1.0.1` while preserving each generated DAG's
observable behaviour (dag_id, schedule, task_ids, operator
identity, task callable semantics). Airflow itself stays on the
version pinned today.
