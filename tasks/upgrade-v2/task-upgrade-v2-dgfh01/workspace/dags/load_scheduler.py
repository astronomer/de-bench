"""Loader for the scheduler/dataset DAGs."""

from pathlib import Path

from dagfactory import DagFactory

_CONFIG = Path(__file__).parent / "scheduler.yml"

dag_factory = DagFactory(str(_CONFIG))
dag_factory.generate_dags(globals())
