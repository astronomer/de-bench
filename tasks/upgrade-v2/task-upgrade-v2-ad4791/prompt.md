Add two tasks to the existing DAG in `dags/`: one that looks up
the most recent successful run, and one that checks that run's
task states and raises if any of them did not succeed. Use the
Airflow 3 Task SDK for both lookups rather than opening a
database session. Preserve the existing DAG and its task id. The
code must run on the Airflow version this project pins.
