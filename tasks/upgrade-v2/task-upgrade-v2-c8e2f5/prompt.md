This project ships an Airflow plugin alongside its DAGs. Update
the project to run cleanly on Airflow 3.0; the plugin does not
need to keep the surface it had before. The DAGs must keep their
observable behaviour and task ids.
