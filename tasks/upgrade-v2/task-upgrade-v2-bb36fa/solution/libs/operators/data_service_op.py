"""Concrete operator the DAG uses. Inherits BaseDataServiceOperator."""

from __future__ import annotations

from airflow.providers.standard.operators.python import PythonOperator

from libs.operators.base_custom import BaseDataServiceOperator


class DataServiceOperator(BaseDataServiceOperator):
    def execute(self, context):
        self._log_window(context)
        return {"endpoint": self.endpoint, "ok": True}


def make_summary_task(task_id: str, fn) -> PythonOperator:
    """Helper callers use when they want a one-off summary task without
    going through DataServiceOperator's retry/connection plumbing."""
    return PythonOperator(task_id=task_id, python_callable=fn)
