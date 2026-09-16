This project pins `astronomer-cosmos==1.6.0` in `requirements.txt`.
It renders one dbt project as a DAG and has a downstream DAG
scheduled on the dataset that the dbt DAG emits. Land the project
on `astronomer-cosmos==1.14.1` while preserving each generated
DAG's observable behaviour (dag_id, schedule, parser behaviour,
task callable semantics). Airflow stays on the version pinned
today.
