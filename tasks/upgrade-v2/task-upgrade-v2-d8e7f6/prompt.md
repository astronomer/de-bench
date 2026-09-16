This Astro project pins an older Airflow runtime in its
`Dockerfile`. Update the project to run cleanly on Airflow 2.11
— that includes picking a runtime image tag whose Airflow
version is 2.11. Preserve every DAG's observable behaviour and
task ids.
