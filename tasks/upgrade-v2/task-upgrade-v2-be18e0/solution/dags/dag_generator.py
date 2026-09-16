"""Custom internal DAG factory. Reads pipelines.yaml, emits one DAG per entry."""

from pathlib import Path

import yaml
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from libs.etl_helpers import build_callable, default_start_date

_CONFIG = Path(__file__).parent / "pipelines.yaml"


def _make_dag(name: str, schedule: str, tasks: list[dict]) -> DAG:
    dag = DAG(
        dag_id=name,
        start_date=default_start_date(),
        schedule=schedule,
        catchup=False,
    )
    with dag:
        for t in tasks:
            PythonOperator(
                task_id=t["id"],
                python_callable=build_callable(t["handler"]),
            )
    return dag


def _load() -> dict:
    with _CONFIG.open() as f:
        return yaml.safe_load(f)


for _pipeline in _load()["pipelines"]:
    globals()[_pipeline["name"]] = _make_dag(
        _pipeline["name"],
        _pipeline["schedule"],
        _pipeline["tasks"],
    )
