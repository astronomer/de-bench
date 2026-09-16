This project pins `dag-factory==0.22.0` in `requirements.txt` and
uses it to generate a handful of DAGs from two YAML configs. The
team wants to land on `dag-factory==1.0.1` while every generated
DAG keeps its observable behaviour (dag_id, schedule, task_ids,
operator identity, task callable semantics). Airflow itself stays
on the version pinned today.
