This project ships a custom operator that the DAG depends on.
Update the project to run cleanly on Airflow 3.0. Preserve the
custom operator's public signature, every DAG's observable
behaviour, and the task ids.
