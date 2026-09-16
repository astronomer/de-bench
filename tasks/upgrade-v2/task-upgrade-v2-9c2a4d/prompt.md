Add a new DAG to `dags/` containing exactly one task. That task
must check the current run's task states through the Airflow 3
Task SDK and raise if any of them did not succeed. Query the
states via the Task SDK, not by opening a database session.
Preserve the existing DAG and its task id. The DAG must be
loadable on the Airflow version this project pins.
