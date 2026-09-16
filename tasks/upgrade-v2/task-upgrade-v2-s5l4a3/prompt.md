Update this billing pipeline to run cleanly on Airflow 3.0.
Preserve the DAG's task ids, its schedule, and the on-call
paging behaviour the team relies on when an invoice run takes
too long.
