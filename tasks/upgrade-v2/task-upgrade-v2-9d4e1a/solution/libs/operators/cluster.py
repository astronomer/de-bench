"""Cluster-launch operator with a console-link operator-link.

The operator launches a transient compute cluster for one task and
publishes a monitor URL on the task UI via ``ClusterMonitorLink``.
The link reads the cluster id pushed at execute time and renders a
deep link to the platform console.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from airflow.sdk import BaseOperator
from airflow.sdk.bases.operatorlink import BaseOperatorLink

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.types import TaskInstanceKey


_CONSOLE_BASE = "https://console.example.com"


class ClusterMonitorLink(BaseOperatorLink):
    """Console-link surfaced on the task UI for LaunchClusterOperator."""

    name = "Cluster monitor"
    key: ClassVar[str] = "cluster_monitor"

    @property
    def xcom_key(self) -> str:
        return self.key

    @classmethod
    def persist(cls, context: Context, **value: Any) -> None:
        context["ti"].xcom_push(key=cls.key, value=value)

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        from airflow.sdk.execution_time.xcom import XCom

        conf = XCom.get_value(key=self.key, ti_key=ti_key)
        if not conf:
            return ""
        return f"{_CONSOLE_BASE}/clusters/{conf['region']}/{conf['cluster_id']}"


class LaunchClusterOperator(BaseOperator):
    """Launch a transient compute cluster and publish a monitor URL."""

    template_fields = ("cluster_name", "region")
    operator_extra_links = (ClusterMonitorLink(),)

    def __init__(
        self,
        *,
        cluster_name: str,
        region: str,
        instance_type: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.region = region
        self.instance_type = instance_type

    def execute(self, context: Context) -> str:
        cluster_id = f"{self.cluster_name}-{context['ds_nodash']}"
        self.log.info("Launching cluster %s in %s", cluster_id, self.region)
        ClusterMonitorLink.persist(
            context=context,
            cluster_id=cluster_id,
            region=self.region,
        )
        return cluster_id
