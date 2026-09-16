This project is pinned to an older Airflow version in
requirements.txt and ships a DAG that uses pre-3.x imports.
The team wants to land on Airflow 3.0, but not in one jump: work
out the correct upgrade route and pin requirements.txt to the
next version on that route, not to the final target. Update the
DAG so it is ready for 3.0. Keep the DAG's task ids and
observable behaviour.
