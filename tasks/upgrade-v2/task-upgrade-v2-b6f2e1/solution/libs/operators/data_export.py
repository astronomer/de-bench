"""Custom operator that exports a query result to a partner SFTP drop.

Used by the daily compliance-report DAG. Wraps a pre-existing internal
SDK so the DAG body stays declarative.
"""

from __future__ import annotations

from typing import Any

from airflow.sdk import BaseOperator


class DataExportOperator(BaseOperator):
    """Export the result of a SQL query to a partner SFTP drop."""

    template_fields = ("sql", "destination_path")

    def __init__(
        self,
        *,
        sql: str,
        destination_path: str,
        partner_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.destination_path = destination_path
        self.partner_id = partner_id

    def execute(self, context: dict[str, Any]) -> str:
        # In production this would call into the partner SDK; here
        # we just return the path so the DAG demonstrates real
        # observable behaviour.
        self.log.info("Exporting query for partner %s to %s", self.partner_id, self.destination_path)
        return self.destination_path
