This project pins `astronomer-cosmos==0.7.5` in `requirements.txt`
and uses it to render a dbt project as Airflow DAGs. Land it on
`astronomer-cosmos==1.14.1` while preserving each generated DAG's
observable behaviour (dag_id, schedule, tasks, profile resolution).
Airflow stays on the version pinned today.
