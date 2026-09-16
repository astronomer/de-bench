"""Intermediate base operator shared across the data-service operators."""

from __future__ import annotations

from airflow.models import BaseOperator
from airflow.utils.dates import days_ago


class BaseDataServiceOperator(BaseOperator):
    """Shared connection + retry behaviour for data-service operators."""

    default_start_date = days_ago(1)

    def __init__(self, *, endpoint: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint

    def execute(self, context):  # pragma: no cover - overridden by subclasses
        raise NotImplementedError

    def _log_window(self, context) -> None:
        self.log.info("running for execution_date=%s", context["execution_date"])
