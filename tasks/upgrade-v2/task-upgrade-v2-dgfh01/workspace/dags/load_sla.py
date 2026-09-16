"""Loader for the SLA-monitored DAGs."""

from pathlib import Path

from dagfactory import DagFactory

_CONFIG = Path(__file__).parent / "sla.yml"

dag_factory = DagFactory(str(_CONFIG))
dag_factory.generate_dags(globals())
